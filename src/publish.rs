//! Publish one message to a ROS 2 topic from SQL.
//!
//! A call runs in the invoking backend, like `CALL subscribe`: it creates a
//! fresh ROS context, resolves or accepts the message type, and sends one
//! message with the default reliable topic QoS. No background worker or
//! persistent registry is involved, and the graph worker does not need to be
//! preloaded.
mod json;

use pgrx::prelude::*;
use rclrs::{
    CreateBasicExecutor, DynamicMessage, DynamicPublisher, Executor, IntoNodeOptions,
    IntoPrimitiveOptions, MessageTypeName, Node, RclrsErrorFilter, SpinOptions,
};
use serde_json::Value;
use std::time::{Duration, Instant};

/// How long graph discovery may take before the message type must be supplied.
const DISCOVERY: Duration = Duration::from_secs(5);
/// How long a publisher waits for a matching subscription before sending.
const MATCHING: Duration = Duration::from_secs(2);
/// How long to keep spinning after publishing so DDS can serialize the sample.
const FLUSH: Duration = Duration::from_millis(100);

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

fn run(topic: &str, message_type: Option<&str>, message: &Value) -> Result<String, String> {
    crate::naming::validate_topic(topic)?;
    let context = crate::ros_context().map_err(|error| crate::describe(&error))?;
    let mut executor = context.create_basic_executor();
    let name = format!("pg_ros2_publisher_{}", std::process::id());
    let node = executor
        .create_node(
            name.as_str()
                .enable_rosout(false)
                .start_parameter_services(false),
        )
        .map_err(|error| crate::describe(&error))?;
    let resolved = match message_type {
        Some(message_type) => message_type.to_owned(),
        None => discover(&node, topic, &mut executor)?,
    };
    let kind: MessageTypeName = resolved
        .as_str()
        .try_into()
        .map_err(|error: rclrs::DynamicMessageError| crate::describe(&error))?;
    let mut dynamic = DynamicMessage::new(kind.clone()).map_err(|error| crate::describe(&error))?;
    json::decode(&mut dynamic, message)?;
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
    wait_for_subscriber(&publisher, &mut executor)?;
    publisher
        .publish(dynamic)
        .map_err(|error| crate::describe(&error))?;
    flush(&mut executor)?;
    Ok(resolved)
}

/// Wait for the graph to advertise a unique type for `topic`.
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

/// Keep draining the executor so the RMW can finish serializing the sample.
fn flush(executor: &mut Executor) -> Result<(), String> {
    let deadline = Instant::now() + FLUSH;
    while Instant::now() < deadline {
        spin(executor)?;
    }
    Ok(())
}

fn spin(executor: &mut Executor) -> Result<(), String> {
    pgrx::check_for_interrupts!();
    executor
        .spin(SpinOptions::new().timeout(Duration::from_millis(100)))
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
