use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use rclrs::{
    Context, CreateBasicExecutor, InitOptions, IntoNodeOptions, Node, RclrsError, RclrsErrorFilter,
    SpinOptions,
};
use std::error::Error;
use std::ffi::{c_char, CStr, CString};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

mod naming;
mod parameters;
mod publish;
mod subscriptions;

::pgrx::pg_module_magic!(name, version);

static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

/// Seconds a successfully discovered but empty graph may persist before the
/// worker rebuilds its ROS context. Zero disables rebuilding.
static ROS_REINIT_AFTER: GucSetting<i32> = GucSetting::<i32>::new(30);

extension_sql!(
    r#"
CREATE TABLE @extschema@.nodes (
    node_name text NOT NULL,
    namespace text NOT NULL,
    refreshed_at timestamptz NOT NULL
);
CREATE TABLE @extschema@.topics (
    topic_name text NOT NULL,
    message_type text NOT NULL,
    refreshed_at timestamptz NOT NULL
);
CREATE TABLE @extschema@.worker_status (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    worker_pid integer NOT NULL,
    last_checked timestamptz NOT NULL,
    last_refreshed timestamptz,
    last_error text
);
CREATE TABLE @extschema@.parameters (
    node_name text NOT NULL,
    namespace text NOT NULL,
    parameter_name text NOT NULL,
    parameter_type text NOT NULL,
    value jsonb NOT NULL,
    refreshed_at timestamptz NOT NULL,
    PRIMARY KEY (node_name, namespace, parameter_name)
);
CREATE TABLE @extschema@.parameter_status (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    worker_pid integer NOT NULL,
    last_checked timestamptz NOT NULL,
    last_refreshed timestamptz,
    last_error text
);
"#,
    name = "graph_tables",
);

// Register only in the postmaster; ROS must never be initialized before fork.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // A plain CREATE EXTENSION may load the library without preloading. In that
    // case create the tables, but do not define startup-only GUCs or a worker.
    // SAFETY: PostgreSQL calls this entry point on its main thread.
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }
    GucRegistry::define_string_guc(
        c"pg_ros2.database",
        c"Database whose ROS graph tables the worker maintains.",
        c"Install pg_ros2 in this database. Changing it requires a server restart.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_ros2.ros_reinit_after",
        c"Seconds of an empty ROS graph before the worker rebuilds its ROS context.",
        c"A DDS participant created before the network was usable never discovers peers, so the worker rebuilds its context after this many seconds of successful but empty discovery. Zero disables rebuilding.",
        &ROS_REINIT_AFTER,
        0,
        86_400,
        GucContext::Sighup,
        GucFlags::default(),
    );
    BackgroundWorkerBuilder::new("pg_ros2 graph worker")
        .set_library("pg_ros2")
        .set_function("graph_worker_main")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn graph_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let database = DATABASE.get().expect("pg_ros2.database must be set");
    let database = database.to_str().expect("pg_ros2.database must be UTF-8");
    BackgroundWorker::connect_worker_to_spi(Some(database), None);
    BackgroundWorker::transaction(|| {
        Spi::run(
            "SET search_path = pg_catalog; SET lock_timeout = '2s'; SET statement_timeout = '5s'",
        )
        .unwrap();
    });
    pgrx::log!("pg_ros2 event=worker_started database={}", database);
    // run_observer owns all ROS resources, so they drop before reporting an error.
    if let Err(err) = run_observer() {
        let message = describe(&err);
        BackgroundWorker::transaction(|| {
            persist_snapshot(Err(message.as_str()), None, None);
        });
        pgrx::error!("pg_ros2 event=observer_failed error={}", message);
    }
    pgrx::log!("pg_ros2 event=worker_stopped database={}", database);
}

#[derive(Debug, PartialEq, Eq)]
struct GraphSnapshot {
    nodes: Vec<(String, String)>,
    topics: Vec<(String, String)>,
}

impl GraphSnapshot {
    fn read(node: &Node) -> Result<Self, RclrsError> {
        let observer_name = node.name();
        let observer_namespace = node.namespace();
        let mut nodes: Vec<_> = node
            .get_node_names()?
            .into_iter()
            .filter(|info| info.name != observer_name || info.namespace != observer_namespace)
            .map(|info| (info.name, info.namespace))
            .collect();
        let mut topics: Vec<_> = node
            .get_topic_names_and_types()?
            .into_iter()
            .flat_map(|(name, types)| types.into_iter().map(move |kind| (name.clone(), kind)))
            .collect();
        nodes.sort();
        topics.sort();
        Ok(Self { nodes, topics })
    }
}

/// Create a ROS context for a forked PostgreSQL process.
///
/// Creating a context also configures rcl's logging backend. The default
/// external library, rcl_logging_spdlog, resolves its directory from
/// `ROS_LOG_DIR`, then `ROS_HOME`, then `~/.ros`, and `rcutils_expand_user`
/// fails when the process has no usable `HOME`. That is the normal state of a
/// server started by systemd or a container, and the failure surfaces as
/// `RCL_LOGGING_RET_ERROR`, which is numerically `RCL_RET_TIMEOUT` (2). The
/// context creation then fails with the misleading
/// `Timeout occurred (RCL_RET_TIMEOUT)` observed in the server log.
///
/// PostgreSQL is this extension's log sink, so ask rcl to skip the external
/// library instead of depending on the server's environment. The console
/// handler stays enabled, so ROS messages still reach the server log, and
/// `rcl_logging_fini` skips the external shutdown.
pub(crate) fn ros_context() -> Result<Context, RclrsError> {
    let args = ["--ros-args", "--disable-external-lib-logs"].map(str::to_owned);
    Context::new(args, InitOptions::default())
}

/// Name of the RMW implementation rcl resolved, for example rmw_fastrtps_cpp.
///
/// rclrs does not re-export this, so declare the C entry point that
/// rmw_implementation provides and that rclrs already links.
fn rmw_implementation() -> String {
    unsafe extern "C" {
        fn rmw_get_implementation_identifier() -> *const c_char;
    }
    // SAFETY: called after Context::new, so rcl has initialized the RMW. The
    // result points at a static string owned by the RMW implementation.
    unsafe {
        let identifier = rmw_get_implementation_identifier();
        if identifier.is_null() {
            "unknown".to_owned()
        } else {
            CStr::from_ptr(identifier).to_string_lossy().into_owned()
        }
    }
}

/// Render an error together with its causes.
///
/// rclrs keeps its top-level messages deliberately terse: a failed dynamic
/// message always prints as "Could not create dynamic message". The actionable
/// reason is carried in the `source` chain, for example a message package that
/// `AMENT_PREFIX_PATH` cannot resolve, so include that chain in every
/// operator-facing message.
pub(crate) fn describe(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Longest interval between ROS context rebuilds, reached after repeated
/// rebuilds that still discovered nothing.
const MAX_REINIT_AFTER: Duration = Duration::from_secs(600);

/// Why one ROS session ended.
enum SessionOutcome {
    /// The postmaster asked the worker to stop, so the worker exits.
    Terminated,
    /// Discovery stayed empty for the configured interval, so the caller rebuilds
    /// the ROS context. `saw_graph` reports whether this session ever discovered
    /// a peer, which decides how far the caller backs off.
    Reinitialize { saw_graph: bool },
}

/// The configured rebuild interval, or `None` when rebuilding is disabled.
fn configured_reinit_after() -> Option<Duration> {
    let seconds = ROS_REINIT_AFTER.get();
    if seconds > 0 {
        Some(Duration::from_secs(seconds as u64))
    } else {
        None
    }
}

/// Grow the backoff while discovery keeps coming back empty, but start over
/// after a session that did discover peers: the network was usable then, so the
/// next failure should be retried promptly. Re-reads the setting so turning
/// rebuilding off takes effect without a server restart.
fn next_reinit_after(current: Option<Duration>, saw_graph: bool) -> Option<Duration> {
    let configured = configured_reinit_after()?;
    if saw_graph {
        return Some(configured);
    }
    match current {
        Some(value) => Some((value * 2).min(MAX_REINIT_AFTER)),
        None => Some(configured),
    }
}

fn run_observer() -> Result<(), RclrsError> {
    // This state survives a ROS context rebuild, so a graph or parameter set
    // that did not change is not written again.
    let mut previous: Option<GraphSnapshot> = None;
    let mut extension_oid: Option<pg_sys::Oid> = None;
    let mut previous_parameters: Option<Vec<parameters::Parameter>> = None;
    let mut parameter_extension: Option<pg_sys::Oid> = None;
    let mut reinit_after = configured_reinit_after();
    loop {
        match run_session(
            &mut previous,
            &mut extension_oid,
            &mut previous_parameters,
            &mut parameter_extension,
            reinit_after,
        )? {
            SessionOutcome::Terminated => return Ok(()),
            SessionOutcome::Reinitialize { saw_graph } => {
                let blind_for = reinit_after.unwrap_or_default().as_secs();
                reinit_after = next_reinit_after(reinit_after, saw_graph);
                // A context created before the network was usable never joins
                // DDS discovery, so rebuild it instead of waiting forever.
                pgrx::log!(
                    "pg_ros2 event=ros_context_rebuilding blind_for={}s next_after={}s",
                    blind_for,
                    reinit_after.unwrap_or_default().as_secs()
                );
            }
        }
    }
}

/// Run one ROS participant until PostgreSQL asks the worker to stop or until an
/// empty graph shows that the participant never joined discovery.
fn run_session(
    previous: &mut Option<GraphSnapshot>,
    extension_oid: &mut Option<pg_sys::Oid>,
    previous_parameters: &mut Option<Vec<parameters::Parameter>>,
    parameter_extension: &mut Option<pg_sys::Oid>,
    reinit_after: Option<Duration>,
) -> Result<SessionOutcome, RclrsError> {
    let context = ros_context()?;
    // Record the DDS domain the observer actually joined. It comes from the
    // server process environment, so a domain set only in a client shell or a
    // docker exec is invisible here and leaves the node undiscoverable.
    pgrx::log!(
        "pg_ros2 event=ros_context_ready domain_id={} rmw={}",
        context.domain_id(),
        rmw_implementation()
    );
    let mut executor = context.create_basic_executor();
    let name = format!("pg_ros2_worker_{}", std::process::id());
    let node = executor.create_node(
        name.as_str()
            .enable_rosout(false)
            .start_parameter_services(false),
    )?;
    let dirty = Arc::new(AtomicBool::new(true));
    let notify_dirty = Arc::clone(&dirty);
    // The callback never uses PostgreSQL. rclrs also rechecks every second to
    // cover graph-notification races. Keep the promise alive for the worker's life.
    let _graph_listener =
        node.notify_on_graph_change_with_period(Duration::from_secs(1), move || {
            notify_dirty.store(true, Ordering::Release);
            false
        });
    let startup = Instant::now() + Duration::from_secs(1);
    let mut last_error = None;
    let mut waiting_logged = false;
    let mut next_parameters = startup;
    let mut parameter_poll: Option<parameters::Poll> = None;
    // A participant created before the network was usable can stay isolated for
    // the worker's whole life: rclrs re-reads the graph but never rebuilds the
    // participant. Time a successful but empty graph so the caller can replace
    // this session.
    let mut empty_since: Option<Instant> = None;
    let mut saw_graph = false;
    while BackgroundWorker::wait_latch(Some(Duration::ZERO)) {
        // A timeout is the normal end of our bounded spin, not a ROS failure.
        executor
            .spin(SpinOptions::new().timeout(Duration::from_millis(100)))
            .timeout_ok()
            .first_error()?;
        if let Some(poll) = &mut parameter_poll {
            let result = poll.tick();
            if !matches!(result, Ok(None)) {
                let result = result.map(Option::unwrap);
                let previous_rows = previous_parameters.as_deref();
                let current_extension = *parameter_extension;
                let persisted = BackgroundWorker::transaction(|| {
                    parameters::persist(
                        result.as_deref().map_err(String::as_str),
                        previous_rows,
                        current_extension,
                    )
                });
                *parameter_extension = persisted;
                match result {
                    Ok(snapshot) => *previous_parameters = Some(snapshot),
                    Err(error) => {
                        pgrx::warning!("pg_ros2 event=parameter_discovery_failed error={}", error)
                    }
                }
                parameter_poll = None;
                next_parameters = Instant::now() + Duration::from_secs(5);
            }
        }
        if Instant::now() < startup || !dirty.swap(false, Ordering::AcqRel) {
            continue;
        }
        let snapshot = GraphSnapshot::read(&node);
        let error = snapshot.as_ref().err().map(|err| describe(err));
        if error != last_error {
            if let Some(message) = &error {
                pgrx::warning!("pg_ros2 event=discovery_failed error={}", message);
            } else {
                pgrx::log!("pg_ros2 event=discovery_recovered");
            }
        }
        let outcome = snapshot.as_ref().map_err(|_| error.as_deref().unwrap());
        let previous_snapshot = previous.as_ref();
        let current_extension = *extension_oid;
        let installed = BackgroundWorker::transaction(|| {
            persist_snapshot(outcome, previous_snapshot, current_extension)
        });
        if installed.is_some() {
            match &snapshot {
                Ok(value) if value.nodes.is_empty() && value.topics.is_empty() => {
                    if empty_since.is_none() {
                        empty_since = Some(Instant::now());
                    }
                }
                Ok(_) => {
                    empty_since = None;
                    saw_graph = true;
                }
                // A failed read keeps the last snapshot and is already reported
                // through `last_error`; it says nothing about isolation.
                Err(_) => empty_since = None,
            }
            if let Ok(snapshot) = snapshot {
                if parameter_poll.is_none() && Instant::now() >= next_parameters {
                    match parameters::Poll::start(&node, &snapshot.nodes) {
                        Ok(poll) => parameter_poll = Some(poll),
                        Err(error) => {
                            let previous_rows = previous_parameters.as_deref();
                            let current_extension = *parameter_extension;
                            let persisted = BackgroundWorker::transaction(|| {
                                parameters::persist(Err(&error), previous_rows, current_extension)
                            });
                            *parameter_extension = persisted;
                            next_parameters = Instant::now() + Duration::from_secs(5);
                        }
                    }
                }
                *previous = Some(snapshot);
            }
            waiting_logged = false;
        } else {
            // The extension is absent, so the cache is not readable yet. An
            // empty graph is expected here and must not rebuild the context.
            empty_since = None;
            *previous = None;
            *previous_parameters = None;
            *parameter_extension = None;
            parameter_poll = None;
            if !waiting_logged {
                pgrx::log!("pg_ros2 event=waiting_for_extension hint=run_CREATE_EXTENSION_pg_ros2_in_configured_database");
                waiting_logged = true;
            }
        }
        *extension_oid = installed;
        last_error = error;
        // Rebuild the ROS context instead of spinning forever on a participant
        // that never joined discovery.
        let blind = empty_since.zip(reinit_after);
        if blind.is_some_and(|(since, threshold)| since.elapsed() >= threshold) {
            return Ok(SessionOutcome::Reinitialize { saw_graph });
        }
    }
    Ok(SessionOutcome::Terminated)
}

// A bare SELECT always produces exactly one row. The inner join this replaced
// returns an empty tuple table when the extension is absent, which pgrx reports
// as `SpiError::InvalidPosition`; the old caller unwrapped that into a worker
// crash before `CREATE EXTENSION` ran. The left join keeps it a normal `None`.
const EXTENSION_LOOKUP: &str = "\
    SELECT e.oid, pg_catalog.quote_ident(n.nspname) \
    FROM (SELECT 1) AS singleton \
    LEFT JOIN pg_catalog.pg_extension e ON e.extname::text = $1 \
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace";

pub(crate) fn installed_extension() -> Option<(pg_sys::Oid, String)> {
    lookup_extension("pg_ros2")
}

fn lookup_extension(name: &str) -> Option<(pg_sys::Oid, String)> {
    match Spi::get_two_with_args::<pg_sys::Oid, String>(EXTENSION_LOOKUP, &[name.into()]) {
        Ok((Some(oid), Some(schema))) => Some((oid, schema)),
        Ok(_) => None,
        Err(error) => panic!("pg_ros2 extension lookup failed: {error}"),
    }
}

// Called only on the PostgreSQL main thread inside a transaction. A SQL error
// aborts that transaction and exits the worker; the postmaster restarts it.
fn persist_snapshot(
    snapshot: Result<&GraphSnapshot, &str>,
    previous: Option<&GraphSnapshot>,
    previous_extension: Option<pg_sys::Oid>,
) -> Option<pg_sys::Oid> {
    let (oid, schema) = installed_extension()?;
    let changed =
        snapshot.is_ok_and(|value| previous != Some(value) || previous_extension != Some(oid));
    if let Ok(value) = snapshot {
        if changed {
            // DELETE keeps the previous committed snapshot readable through MVCC.
            Spi::run(&format!(
                "LOCK TABLE {schema}.nodes, {schema}.topics IN SHARE ROW EXCLUSIVE MODE; \
                DELETE FROM {schema}.nodes; DELETE FROM {schema}.topics"
            ))
            .unwrap();
            let (names, namespaces): (Vec<_>, Vec<_>) = value.nodes.iter().cloned().unzip();
            Spi::run_with_args(&format!("INSERT INTO {schema}.nodes \
                SELECT n, ns, statement_timestamp() FROM unnest($1::text[], $2::text[]) AS t(n, ns)"),
                &[names.into(), namespaces.into()]).unwrap();
            let (names, types): (Vec<_>, Vec<_>) = value.topics.iter().cloned().unzip();
            Spi::run_with_args(&format!("INSERT INTO {schema}.topics \
                SELECT n, ty, statement_timestamp() FROM unnest($1::text[], $2::text[]) AS t(n, ty)"),
                &[names.into(), types.into()]).unwrap();
        }
    }
    Spi::run_with_args(&format!(
        "INSERT INTO {schema}.worker_status AS s (singleton, worker_pid, last_checked, last_refreshed, last_error) \
         VALUES (true, pg_backend_pid(), statement_timestamp(), \
             CASE WHEN $1 THEN statement_timestamp() END, $2) \
         ON CONFLICT (singleton) DO UPDATE SET worker_pid = EXCLUDED.worker_pid, \
             last_checked = EXCLUDED.last_checked, \
             last_refreshed = CASE WHEN $1 THEN EXCLUDED.last_refreshed ELSE s.last_refreshed END, \
             last_error = EXCLUDED.last_error"),
        &[changed.into(), snapshot.err().into()]).unwrap();
    Some(oid)
}

#[pg_extern(immutable, parallel_safe)]
fn hello_pg_ros2() -> &'static str {
    "Hello, pg_ros2"
}

/// Put the extension schema on the search path so test SQL can use plain table
/// names; the control file pins that schema to `ros2`.
#[cfg(any(test, feature = "pg_test"))]
pub(crate) fn use_extension_schema() {
    let (_, schema) = installed_extension().expect("pg_ros2 is installed");
    Spi::run(&format!("SET LOCAL search_path = {schema}, pg_catalog")).unwrap();
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_snapshot_reconciliation() {
        use_extension_schema();
        let first = GraphSnapshot {
            nodes: vec![
                ("duplicate".into(), "/".into()),
                ("duplicate".into(), "/".into()),
            ],
            topics: vec![
                ("/quoted'_topic".into(), "test/msg/A".into()),
                ("/quoted'_topic".into(), "test/msg/B".into()),
            ],
        };
        let oid = persist_snapshot(Ok(&first), None, None);
        assert!(oid.is_some());
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM nodes"),
            Ok(Some(2))
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM topics"),
            Ok(Some(2))
        );
        // Rechecking an unchanged graph must not rewrite either table.
        Spi::run("CREATE TEMP TABLE old_ctids AS SELECT ctid AS tid FROM topics").unwrap();
        persist_snapshot(Ok(&first), Some(&first), oid);
        assert_eq!(
            Spi::get_one::<bool>(
                "SELECT NOT EXISTS (SELECT ctid FROM topics EXCEPT SELECT tid FROM old_ctids)"
            ),
            Ok(Some(true))
        );
        // Discovery failure records an error and preserves both snapshots.
        persist_snapshot(Err("injected discovery failure"), Some(&first), oid);
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM topics"),
            Ok(Some(2))
        );
        assert_eq!(
            Spi::get_one::<String>("SELECT last_error FROM worker_status"),
            Ok(Some("injected discovery failure".into()))
        );
        let empty = GraphSnapshot {
            nodes: vec![],
            topics: vec![],
        };
        persist_snapshot(Ok(&empty), Some(&first), oid);
        assert_eq!(Spi::get_one::<bool>("SELECT NOT EXISTS (SELECT FROM nodes) AND NOT EXISTS (SELECT FROM topics) AND (SELECT last_error IS NULL FROM worker_status)"), Ok(Some(true)));
    }

    // Regression: an uninstalled extension used to reach pgrx's InvalidPosition
    // error on an empty tuple table, crashing the worker before CREATE EXTENSION.
    #[pg_test]
    fn test_extension_lookup_tolerates_absent_extension() {
        assert_eq!(lookup_extension("pg_ros2_absent"), None);
        assert!(installed_extension().is_some());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
