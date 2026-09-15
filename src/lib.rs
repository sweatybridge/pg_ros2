use pgrx::prelude::*;
use rclrs::{Context, CreateBasicExecutor, InitOptions, IntoNodeOptions, Node, RclrsError};
use std::time::{Duration, Instant};

::pgrx::pg_module_magic!(name, version);

#[pg_extern(immutable, parallel_safe)]
fn hello_pg_ros2() -> &'static str {
    "Hello, pg_ros2"
}

/// Take a snapshot of discovered node names and namespaces.
#[pg_extern(volatile, parallel_unsafe)]
fn ros2_nodes(
    discovery_ms: default!(i32, 250),
) -> TableIterator<'static, (name!(node_name, String), name!(namespace, String))> {
    check_access_and_wait(discovery_ms);
    let rows = with_node(discovery_ms, |node| {
        let mut rows: Vec<_> = node
            .get_node_names()?
            .into_iter()
            .map(|info| (info.name, info.namespace))
            .collect();
        rows.sort();
        Ok(rows)
    })
    .unwrap_or_else(|err| pgrx::error!("ROS 2 node discovery failed: {}", err));
    TableIterator::new(rows)
}

/// Take a snapshot with one row per discovered topic/type pair.
#[pg_extern(volatile, parallel_unsafe)]
fn ros2_topics(
    discovery_ms: default!(i32, 250),
) -> TableIterator<'static, (name!(topic_name, String), name!(message_type, String))> {
    check_access_and_wait(discovery_ms);
    let rows = with_node(discovery_ms, |node| {
        let mut rows: Vec<_> = node
            .get_topic_names_and_types()?
            .into_iter()
            .flat_map(|(topic, types)| types.into_iter().map(move |kind| (topic.clone(), kind)))
            .collect();
        rows.sort();
        Ok(rows)
    })
    .unwrap_or_else(|err| pgrx::error!("ROS 2 topic discovery failed: {}", err));
    TableIterator::new(rows)
}

fn check_access_and_wait(discovery_ms: i32) {
    // A function-level check also covers PUBLIC's default EXECUTE privilege.
    // SAFETY: Called only on the PostgreSQL backend thread by a SQL function.
    if !unsafe { pgrx::pg_sys::superuser() } {
        pgrx::error!("ROS 2 discovery requires a PostgreSQL superuser");
    }
    if !(0..=2000).contains(&discovery_ms) {
        pgrx::error!("discovery_ms must be between 0 and 2000");
    }
}

fn with_node<T>(
    discovery_ms: i32,
    read: impl FnOnce(&Node) -> Result<T, RclrsError>,
) -> Result<T, RclrsError> {
    // No postmaster initialization, global ROS state, or executor callbacks.
    // Keep ROS resources inside this scope so they drop before SQL materialization
    // or conversion of a returned ROS error into a PostgreSQL ERROR.
    let context = Context::new([], InitOptions::default())?;
    let executor = context.create_basic_executor();
    let name = format!("pg_ros2_{}", std::process::id());
    let node = executor.create_node(
        name.as_str()
            .enable_rosout(false)
            .start_parameter_services(false),
    )?;
    // DDS graph discovery runs in middleware threads; no executor spin is needed.
    let deadline = Instant::now() + Duration::from_millis(discovery_ms as u64);
    while Instant::now() < deadline {
        pgrx::check_for_interrupts!();
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10)),
        );
    }
    pgrx::check_for_interrupts!();
    read(&node)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello() {
        assert_eq!(
            Spi::get_one::<String>("SELECT hello_pg_ros2()"),
            Ok(Some("Hello, pg_ros2".to_owned()))
        );
    }

    #[pg_test]
    fn test_discover_self_and_recreate_context() {
        for _ in 0..3 {
            assert_eq!(
                Spi::get_one::<bool>(
                    "SELECT EXISTS (SELECT FROM ros2_nodes(0) \
                     WHERE node_name = 'pg_ros2_' || pg_backend_pid()::text AND namespace = '/')"
                ),
                Ok(Some(true))
            );
        }
    }

    #[pg_test]
    fn test_topic_snapshot() {
        assert!(Spi::get_one::<i64>("SELECT count(*) FROM ros2_topics(0)")
            .unwrap()
            .is_some());
    }

    #[pg_test(error = "discovery_ms must be between 0 and 2000")]
    fn test_negative_wait() {
        Spi::run("SELECT * FROM ros2_nodes(-1)").unwrap();
    }

    #[pg_test(error = "discovery_ms must be between 0 and 2000")]
    fn test_excessive_wait() {
        Spi::run("SELECT * FROM ros2_topics(2001)").unwrap();
    }

    #[pg_test(error = "ROS 2 discovery requires a PostgreSQL superuser")]
    fn test_unprivileged_discovery() {
        Spi::run("CREATE ROLE ros2_test_unprivileged; SET LOCAL ROLE ros2_test_unprivileged")
            .unwrap();
        Spi::run("SELECT * FROM ros2_nodes(0)").unwrap();
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
