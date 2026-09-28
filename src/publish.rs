//! Publish one message to a ROS 2 topic from SQL.
//!
//! The first publish in a backend builds a ROS context, node, executor, and
//! publisher, and keeps them for the rest of that backend's life. Later calls
//! reuse the DDS participant and its loaded type-support libraries instead of
//! paying for them again per statement. No background worker or persistent
//! registry is involved, and the graph worker does not need to be preloaded.
mod json;

use pgrx::prelude::*;
use rclrs::{
    Context, CreateBasicExecutor, DynamicMessage, DynamicPublisher, Executor, IntoNodeOptions,
    IntoPrimitiveOptions, MessageTypeName, Node, RclrsErrorFilter, SpinOptions,
};
use serde_json::Value;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long graph discovery may take before the message type must be supplied.
const DISCOVERY: Duration = Duration::from_secs(5);
/// How long the first publish for a publisher waits for a matching subscription.
const MATCHING: Duration = Duration::from_secs(2);
/// How long a newly created publisher keeps spinning so DDS can serialize the
/// sample before the statement ends.
const FLUSH: Duration = Duration::from_millis(100);
/// How long a reused publisher spins after publishing. Its context stays alive
/// between statements, so this only covers handing the sample to the RMW, not
/// any part of teardown.
const CACHED_FLUSH: Duration = Duration::from_millis(10);
/// Longest single executor spin while polling for discovery or a match.
const SPIN: Duration = Duration::from_millis(100);

// The overloads share one SQL name. Both require the message type or infer it
// from the graph; both are revoked from PUBLIC because they load native ROS
// libraries and reach the server's ROS domain.
#[pg_extern(sql = r#"
CREATE FUNCTION @extschema@.publish(topic text, message jsonb)
RETURNS text LANGUAGE c STRICT
AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
REVOKE ALL ON FUNCTION @extschema@.publish(text, jsonb) FROM PUBLIC;
"#)]
fn publish(topic: String, message: pgrx::JsonB) -> String {
    publish_message(&topic, None, &message.0)
}

#[pg_extern(sql = r#"
CREATE FUNCTION @extschema@.publish(topic text, message_type text, message jsonb)
RETURNS text LANGUAGE c STRICT
AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
REVOKE ALL ON FUNCTION @extschema@.publish(text, text, jsonb) FROM PUBLIC;
"#)]
fn publish_typed(topic: String, message_type: String, message: pgrx::JsonB) -> String {
    publish_message(&topic, Some(&message_type), &message.0)
}

fn publish_message(topic: &str, message_type: Option<&str>, message: &Value) -> String {
    match run(topic, message_type, message) {
        Ok(resolved) => resolved,
        Err(error) => pgrx::error!("publish to {topic} failed: {error}"),
    }
}

/// A publisher that later publishes in the same backend reuse.
struct CachedPublisher {
    publisher: DynamicPublisher,
}

/// The ROS resources a backend builds on its first publish and reuses after.
struct PublisherSession {
    node: Node,
    executor: Executor,
    /// One publisher per (topic, message type).
    publishers: HashMap<(String, String), CachedPublisher>,
    /// Kept so the rcl context outlives the node and executor built from it.
    /// Declared last so it drops after them.
    _context: Context,
}

impl PublisherSession {
    fn new() -> Result<Self, String> {
        let context = crate::ros_context().map_err(|error| crate::describe(&error))?;
        let executor = context.create_basic_executor();
        let name = format!("pg_ros2_publisher_{}", std::process::id());
        let node = executor
            .create_node(
                name.as_str()
                    .enable_rosout(false)
                    .start_parameter_services(false),
            )
            .map_err(|error| crate::describe(&error))?;
        Ok(Self {
            node,
            executor,
            publishers: HashMap::new(),
            _context: context,
        })
    }
}

thread_local! {
    /// The publishing session for this backend, created on its first publish.
    ///
    /// An UnsafeCell rather than a RefCell: PostgreSQL longjmps out of
    /// check_for_interrupts when a query is canceled, which can happen while
    /// this cell is borrowed inside a spin. A RefCell borrow flag left set by
    /// that longjmp would make every later publish in the backend fail. A
    /// backend runs its SQL on one thread and publish never re-enters itself,
    /// so no second reference to the cell can exist.
    static SESSION: UnsafeCell<Option<PublisherSession>> = const { UnsafeCell::new(None) };
}

fn with_session<T>(
    action: impl FnOnce(&mut PublisherSession) -> Result<T, String>,
) -> Result<T, String> {
    SESSION.with(|cell| {
        // SAFETY: PostgreSQL executes a backend's SQL on a single thread and a
        // publish cannot re-enter this function, so this is the only live
        // reference to the cell. The reference never escapes action.
        let session = unsafe { &mut *cell.get() };
        if session.is_none() {
            *session = Some(PublisherSession::new()?);
        }
        action(session.as_mut().expect("session was just initialized"))
    })
}

fn run(topic: &str, message_type: Option<&str>, message: &Value) -> Result<String, String> {
    crate::naming::validate_topic(topic)?;
    with_session(|session| publish_with_session(session, topic, message_type, message))
}

fn publish_with_session(
    session: &mut PublisherSession,
    topic: &str,
    message_type: Option<&str>,
    message: &Value,
) -> Result<String, String> {
    // Clone the node so resolving and creating do not borrow the session while
    // its executor is borrowed mutably for spinning.
    let node = Arc::clone(&session.node);
    let resolved = match message_type {
        Some(message_type) => message_type.to_owned(),
        None => discover(&node, topic, &mut session.executor)?,
    };
    let kind: MessageTypeName = resolved
        .as_str()
        .try_into()
        .map_err(|error: rclrs::DynamicMessageError| crate::describe(&error))?;
    let key = (topic.to_owned(), resolved.clone());
    // A cached publisher skips discovery, publisher creation, and the 100ms
    // post-create flush. It still checks for a matching subscriber, which
    // costs one rcl call and returns immediately once one is matched, so a
    // subscriber that restarted is waited for just like the first publish.
    if let Some(cached) = session.publishers.get(&key) {
        let dynamic = build_message(&kind, message)?;
        wait_for_subscriber(&cached.publisher, &mut session.executor)?;
        cached
            .publisher
            .publish(dynamic)
            .map_err(|error| crate::describe(&error))?;
        flush(&mut session.executor, CACHED_FLUSH)?;
        return Ok(resolved);
    }
    let dynamic = build_message(&kind, message)?;
    let publisher = node
        .create_dynamic_publisher(kind, topic.keep_last(10))
        .map_err(|error| {
            // rclrs resolves type support libraries through AMENT_PREFIX_PATH,
            // which is fixed when the database server starts.
            format!(
                "{} (message packages are resolved from the database server's \
                 AMENT_PREFIX_PATH; start the server with the ROS environment sourced)",
                crate::describe(&error)
            )
        })?;
    wait_for_subscriber(&publisher, &mut session.executor)?;
    publisher
        .publish(dynamic)
        .map_err(|error| crate::describe(&error))?;
    flush(&mut session.executor, FLUSH)?;
    session
        .publishers
        .insert(key, CachedPublisher { publisher });
    Ok(resolved)
}

fn build_message(kind: &MessageTypeName, message: &Value) -> Result<DynamicMessage, String> {
    let mut dynamic = DynamicMessage::new(kind.clone()).map_err(|error| crate::describe(&error))?;
    json::decode(&mut dynamic, message)?;
    Ok(dynamic)
}

/// Wait for the graph to advertise a unique type for the topic.
fn discover(node: &Node, topic: &str, executor: &mut Executor) -> Result<String, String> {
    let deadline = Instant::now() + DISCOVERY;
    loop {
        let topics = node
            .get_topic_names_and_types()
            .map_err(|error| crate::describe(&error))?;
        if let Some(types) = topics.get(topic) {
            if types.len() != 1 {
                return Err("topic has multiple message types; a unique type is required".into());
            }
            return Ok(types[0].clone());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "topic {topic} has no unique advertised message type; \
                 pass the message type explicitly"
            ));
        }
        spin(executor)?;
    }
}

/// Spin until at least one subscription matches, or the short deadline passes.
///
/// DDS discovery is asynchronous, so publishing before a subscriber has
/// matched would lose a one-shot reliable message. A topic with no subscriber
/// still publishes after the wait.
fn wait_for_subscriber(
    publisher: &DynamicPublisher,
    executor: &mut Executor,
) -> Result<(), String> {
    let deadline = Instant::now() + MATCHING;
    loop {
        let matched = publisher
            .get_subscription_count()
            .map_err(|error| crate::describe(&error))?;
        if matched > 0 || Instant::now() >= deadline {
            return Ok(());
        }
        spin(executor)?;
    }
}

/// Keep draining the executor for the given span so the RMW can finish
/// serializing a sample.
fn flush(executor: &mut Executor, total: Duration) -> Result<(), String> {
    let deadline = Instant::now() + total;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        spin_for(executor, remaining.min(SPIN))?;
    }
}

fn spin(executor: &mut Executor) -> Result<(), String> {
    spin_for(executor, SPIN)
}

fn spin_for(executor: &mut Executor, timeout: Duration) -> Result<(), String> {
    pgrx::check_for_interrupts!();
    executor
        .spin(SpinOptions::new().timeout(timeout))
        .timeout_ok()
        .first_error()
        .map_err(|error| crate::describe(&error))?;
    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_publish_topic_names() {
        for topic in ["/chatter", "/robot_1/joint_states", "/_private"] {
            assert_eq!(crate::naming::validate_topic(topic), Ok(()));
        }
        for topic in [
            "",
            "/",
            "relative",
            "/double//slash",
            "/trailing/",
            "/0digit",
            "/bad'quote",
            "/white space",
            "/世界",
        ] {
            assert!(crate::naming::validate_topic(topic).is_err(), "{topic}");
        }
        // Publishing is not bound by PostgreSQL's 63-byte channel limit.
        assert_eq!(
            crate::naming::validate_topic(&format!("/{}", "a".repeat(200))),
            Ok(())
        );
    }
}
