//! A caller-owned ROS subscription. Each notification batch commits during CALL.
mod json;

use pgrx::prelude::*;
use rclrs::{
    Context, CreateBasicExecutor, InitOptions, IntoNodeOptions, IntoPrimitiveOptions,
    RclrsErrorFilter, SpinOptions,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::sync_channel,
    Arc,
};
use std::time::{Duration, Instant};

// No SECURITY DEFINER or SET clause: both would forbid transaction control.
#[pg_extern(sql = r#"
CREATE PROCEDURE @extschema@.subscribe(topic text)
LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
REVOKE ALL ON PROCEDURE @extschema@.subscribe(text) FROM PUBLIC;
"#)]
fn subscribe(topic: String, fcinfo: pg_sys::FunctionCallInfo) {
    if let Err(error) = validate_topic(&topic) {
        pgrx::error!("{}", error);
    }
    // SAFETY: PostgreSQL supplies fcinfo. Only a non-atomic CALL context permits
    // transaction control. Reject SELECT and explicit transactions before ROS.
    unsafe {
        let context = (*fcinfo).context;
        if context.is_null()
            || (*context).type_ != pg_sys::NodeTag::T_CallContext
            || (*(context.cast::<pg_sys::CallContext>())).atomic
        {
            pgrx::error!(
                "subscribe must be invoked with a top-level CALL outside a transaction block"
            );
        }
        pg_sys::SPI_connect_ext(pg_sys::SPI_OPT_NONATOMIC as i32);
        pg_sys::SPI_commit();
    }
    // Owned Rust data survives SPI_commit; no borrowed PostgreSQL data does.
    let outcome = receive(&topic);
    // SAFETY: the connection remains open on ordinary return. PostgreSQL cleans
    // it up on ERROR/cancellation; do not finish SPI from a Drop implementation.
    unsafe {
        pg_sys::SPI_finish();
    }
    if let Err(error) = outcome {
        pgrx::error!("subscribe failed for {}: {}", topic, error);
    }
}

fn validate_topic(topic: &str) -> Result<(), &'static str> {
    if topic.len() >= pg_sys::NAMEDATALEN as usize {
        return Err("topic exceeds PostgreSQL's 63-byte notification channel limit");
    }
    let Some(relative) = topic.strip_prefix('/') else {
        return Err("topic must be a fully qualified ROS name starting with '/'");
    };
    if relative.split('/').any(|part| {
        let mut bytes = part.bytes();
        !bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return Err("topic must contain nonempty ROS name segments (letters, digits, underscores; no leading digits)");
    }
    Ok(())
}

fn receive(topic: &str) -> Result<(), String> {
    let context = Context::new([], InitOptions::default()).map_err(|e| e.to_string())?;
    let mut executor = context.create_basic_executor();
    let name = format!("pg_ros2_subscriber_{}", std::process::id());
    let node = executor
        .create_node(
            name.as_str()
                .enable_rosout(false)
                .start_parameter_services(false),
        )
        .map_err(|e| e.to_string())?;
    let (sender, receiver) = sync_channel(256);
    let dropped = Arc::new(AtomicU64::new(0));
    let mut subscription = None;
    let mut next_discovery = Instant::now();
    let mut next_report = Instant::now();
    let mut encoding_errors = 0_u64;
    let mut last_encoding_error = None;
    loop {
        // PostgreSQL owns backend signals. Cancellation unwinds ROS resources.
        pgrx::check_for_interrupts!();
        executor
            .spin(SpinOptions::new().timeout(Duration::from_millis(100)))
            .timeout_ok()
            .first_error()
            .map_err(|e| e.to_string())?;
        if subscription.is_none() && Instant::now() >= next_discovery {
            let topics = node
                .get_topic_names_and_types()
                .map_err(|e| e.to_string())?;
            if let Some(types) = topics.get(topic) {
                if types.len() != 1 {
                    return Err(
                        "topic has multiple message types; a unique type is required".into(),
                    );
                }
                let message_type = types[0].clone();
                let kind = message_type
                    .as_str()
                    .try_into()
                    .map_err(|e: rclrs::DynamicMessageError| e.to_string())?;
                let callback_topic = topic.to_owned();
                let sender = sender.clone();
                let dropped = Arc::clone(&dropped);
                let sequence = AtomicU64::new(0);
                subscription = Some(
                    node.create_dynamic_subscription(
                        kind,
                        topic.best_effort().keep_last(10),
                        move |message, _| {
                            let payload = json::payload(
                                &callback_topic,
                                &message_type,
                                sequence.fetch_add(1, Ordering::Relaxed),
                                &message.view(),
                            );
                            if sender.try_send(payload).is_err() {
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        },
                    )
                    .map_err(|e| e.to_string())?,
                );
                pgrx::notice!("subscribed to {}; cancel this CALL to stop", topic);
            }
            next_discovery = Instant::now() + Duration::from_secs(1);
        }
        let mut notified = false;
        for payload in receiver.try_iter().take(256) {
            match payload {
                Ok(payload) => {
                    // Nested atomic SPI closes before the outer connection commits.
                    Spi::connect_mut(|client| {
                        // Convert parameters inside the short-lived nested SPI
                        // context, not the outer context that survives commits.
                        client
                            .update(
                                "SELECT pg_catalog.pg_notify($1, $2)",
                                None,
                                &[topic.into(), payload.into()],
                            )
                            .unwrap();
                    });
                    notified = true;
                }
                Err(error) => {
                    encoding_errors += 1;
                    last_encoding_error = Some(error);
                }
            }
        }
        if notified {
            // SAFETY: the outer SPI connection is non-atomic. No SPI clients,
            // results, snapshots, or transaction-owned data live here.
            unsafe {
                pg_sys::SPI_commit();
            }
        }
        if Instant::now() >= next_report {
            let overflow = dropped.swap(0, Ordering::Relaxed);
            if overflow > 0 || encoding_errors > 0 {
                pgrx::warning!(
                    "pg_ros2 topic={} queue_drops={} encoding_drops={} last_error={}",
                    topic,
                    overflow,
                    encoding_errors,
                    last_encoding_error.unwrap_or("none")
                );
            }
            encoding_errors = 0;
            last_encoding_error = None;
            next_report = Instant::now() + Duration::from_secs(1);
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_topic_channel_names() {
        for topic in ["/chatter", "/robot_1/joint_states", "/_private"] {
            assert_eq!(validate_topic(topic), Ok(()));
        }
        assert_eq!(validate_topic(&format!("/{}", "a".repeat(62))), Ok(()));
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
            assert!(validate_topic(topic).is_err(), "{topic}");
        }
        assert!(validate_topic(&format!("/{}", "a".repeat(63))).is_err());
    }
}
