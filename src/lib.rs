use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use rclrs::{
    Context, CreateBasicExecutor, InitOptions, IntoNodeOptions, Node, RclrsError, RclrsErrorFilter,
    SpinOptions,
};
use std::ffi::CString;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

mod parameters;
mod subscriptions;

::pgrx::pg_module_magic!(name, version);

static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

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
        let message = err.to_string();
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

fn run_observer() -> Result<(), RclrsError> {
    let context = Context::new([], InitOptions::default())?;
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
    let mut previous = None;
    let mut extension_oid = None;
    let mut last_error = None;
    let mut waiting_logged = false;
    let mut previous_parameters = None;
    let mut parameter_extension = None;
    let mut next_parameters = startup;
    while BackgroundWorker::wait_latch(Some(Duration::ZERO)) {
        // A timeout is the normal end of our bounded spin, not a ROS failure.
        executor
            .spin(SpinOptions::new().timeout(Duration::from_millis(100)))
            .timeout_ok()
            .first_error()?;
        if Instant::now() < startup || !dirty.swap(false, Ordering::AcqRel) {
            continue;
        }
        let snapshot = GraphSnapshot::read(&node);
        let error = snapshot.as_ref().err().map(ToString::to_string);
        if error != last_error {
            if let Some(message) = &error {
                pgrx::warning!("pg_ros2 event=discovery_failed error={}", message);
            } else {
                pgrx::log!("pg_ros2 event=discovery_recovered");
            }
        }
        let outcome = snapshot.as_ref().map_err(|_| error.as_deref().unwrap());
        let installed = BackgroundWorker::transaction(|| {
            persist_snapshot(outcome, previous.as_ref(), extension_oid)
        });
        if installed.is_some() {
            if let Ok(snapshot) = snapshot {
                if Instant::now() >= next_parameters {
                    let parameters = parameters::read(&node, &mut executor, &snapshot.nodes);
                    parameter_extension = BackgroundWorker::transaction(|| {
                        parameters::persist(
                            parameters.as_deref().map_err(String::as_str),
                            previous_parameters.as_deref(),
                            parameter_extension,
                        )
                    });
                    if let Err(message) = &parameters {
                        pgrx::warning!(
                            "pg_ros2 event=parameter_discovery_failed error={}",
                            message
                        );
                    } else {
                        previous_parameters = parameters.ok();
                    }
                    next_parameters = Instant::now() + Duration::from_secs(5);
                }
                previous = Some(snapshot);
            }
            waiting_logged = false;
        } else {
            previous = None;
            previous_parameters = None;
            parameter_extension = None;
            if !waiting_logged {
                pgrx::log!("pg_ros2 event=waiting_for_extension hint=run_CREATE_EXTENSION_pg_ros2_in_configured_database");
                waiting_logged = true;
            }
        }
        extension_oid = installed;
        last_error = error;
    }
    Ok(())
}

// Called only on the PostgreSQL main thread inside a transaction. A SQL error
// aborts that transaction and exits the worker; the postmaster restarts it.
fn persist_snapshot(
    snapshot: Result<&GraphSnapshot, &str>,
    previous: Option<&GraphSnapshot>,
    previous_extension: Option<pg_sys::Oid>,
) -> Option<pg_sys::Oid> {
    let (oid, schema) = Spi::get_two::<pg_sys::Oid, String>(
        "SELECT e.oid, pg_catalog.quote_ident(n.nspname) \
         FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = 'pg_ros2'",
    )
    .unwrap();
    let (Some(oid), Some(schema)) = (oid, schema) else {
        return None;
    };
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_snapshot_reconciliation() {
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
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
