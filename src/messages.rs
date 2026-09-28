//! Table-backed ROS subscriptions.
//!
//! A background worker owns one ROS context and subscribes to every topic
//! listed in ros2.subscriptions. Ordinary DML on that table is the control
//! plane; the worker writes the newest message per topic into ros2.messages.
//! This path is independent of the LISTEN/NOTIFY path in subscriptions.rs,
//! which is unchanged, and it is not limited to PostgreSQL's notification
//! payload size.
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use rclrs::{
    CreateBasicExecutor, DynamicSubscription, IntoNodeOptions, IntoPrimitiveOptions,
    MessageTypeName, Node, RclrsError, RclrsErrorFilter, SpinOptions,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::subscriptions::json;

/// One topic's desired state, as read from ros2.subscriptions.
struct Desired {
    topic: String,
    requested_type: Option<String>,
    idle_seconds: f64,
}

/// The newest message a subscription produced and has not stored yet.
///
/// The callback overwrites the pending slot, so memory is bounded by one
/// message per topic no matter how far behind the drain falls.
#[derive(Default)]
struct Mailbox {
    sequence: u64,
    pending: Option<(u64, String)>,
    error: Option<String>,
}

/// A live subscription and the mailbox its callback fills.
struct Live {
    _subscription: DynamicSubscription,
    message_type: String,
    mailbox: Arc<Mutex<Mailbox>>,
}

/// Entry point for the message worker registered in _PG_init.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn message_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let database = crate::worker_database();
    BackgroundWorker::connect_worker_to_spi(Some(database.as_str()), None);
    BackgroundWorker::transaction(|| {
        Spi::run(
            "SET search_path = pg_catalog; SET lock_timeout = '2s'; SET statement_timeout = '5s'",
        )
        .unwrap();
    });
    pgrx::log!("pg_ros2 event=message_worker_started database={}", database);
    if let Err(error) = run() {
        pgrx::error!(
            "pg_ros2 event=message_worker_failed error={}",
            crate::describe(&error)
        );
    }
    pgrx::log!("pg_ros2 event=message_worker_stopped");
}

fn run() -> Result<(), RclrsError> {
    let context = crate::ros_context()?;
    let mut executor = context.create_basic_executor();
    let name = format!("pg_ros2_messages_{}", std::process::id());
    let node = executor.create_node(
        name.as_str()
            .enable_rosout(false)
            .start_parameter_services(false),
    )?;
    let mut topics: HashMap<String, Live> = HashMap::new();
    let mut next_reconcile = Instant::now();
    let mut next_flush = Instant::now();
    while BackgroundWorker::wait_latch(Some(Duration::ZERO)) {
        // Re-read the intervals so a SIGHUP reload takes effect without a
        // worker restart.
        let message_poll = Duration::from_millis(crate::message_poll_ms() as u64);
        executor
            .spin(SpinOptions::new().timeout(message_poll))
            .timeout_ok()
            .first_error()?;
        let now = Instant::now();
        if now >= next_flush {
            flush(&topics);
            next_flush = Instant::now() + message_poll;
        }
        if now >= next_reconcile {
            reconcile(&node, &mut topics);
            next_reconcile =
                Instant::now() + Duration::from_millis(crate::subscription_poll_ms() as u64);
        }
    }
    Ok(())
}

/// Make the live subscriptions match the table.
fn reconcile(node: &Node, topics: &mut HashMap<String, Live>) {
    let desired = BackgroundWorker::transaction(read_desired);
    let ttl = f64::from(crate::subscription_ttl_seconds());
    let mut wanted = HashSet::with_capacity(desired.len());
    let mut metadata = Vec::new();
    let mut expired = Vec::new();
    for entry in desired {
        if ttl > 0.0 && entry.idle_seconds > ttl {
            expired.push(entry.topic.clone());
            topics.remove(&entry.topic);
            continue;
        }
        wanted.insert(entry.topic.clone());
        if topics.contains_key(&entry.topic) {
            continue;
        }
        match subscribe(
            node,
            &entry.topic,
            entry.requested_type.as_deref(),
            crate::message_max_bytes(),
        ) {
            Ok(live) => {
                metadata.push((entry.topic.clone(), Some(live.message_type.clone()), None));
                topics.insert(entry.topic, live);
            }
            // A topic that is not advertised yet, or whose type support cannot
            // be loaded, is retried on the next reconcile instead of failing
            // the worker.
            Err(error) => metadata.push((entry.topic, None, Some(error))),
        }
    }
    // A row that disappeared stops its subscription and drops its cached
    // message, so a later registration cannot read a stale one.
    let mut removed = Vec::new();
    topics.retain(|topic, _| {
        let keep = wanted.contains(topic);
        if !keep {
            removed.push(topic.clone());
        }
        keep
    });
    if metadata.is_empty() && expired.is_empty() && removed.is_empty() {
        return;
    }
    BackgroundWorker::transaction(|| {
        let Some((_oid, schema)) = crate::installed_extension() else {
            return;
        };
        for (topic, message_type, error) in &metadata {
            match (message_type, error) {
                (Some(kind), _) => {
                    Spi::run_with_args(
                        &format!(
                            "UPDATE {schema}.subscriptions SET message_type = $2, last_error = NULL \
                             WHERE topic_name = $1"
                        ),
                        &[topic.as_str().into(), kind.as_str().into()],
                    )
                    .unwrap();
                }
                (None, Some(error)) => {
                    Spi::run_with_args(
                        &format!(
                            "UPDATE {schema}.subscriptions SET last_error = $2 \
                             WHERE topic_name = $1"
                        ),
                        &[topic.as_str().into(), error.as_str().into()],
                    )
                    .unwrap();
                }
                (None, None) => {}
            }
        }
        for topic in removed.iter().chain(expired.iter()) {
            Spi::run_with_args(
                &format!("DELETE FROM {schema}.messages WHERE topic_name = $1"),
                &[topic.as_str().into()],
            )
            .unwrap();
        }
        for topic in &expired {
            Spi::run_with_args(
                &format!("DELETE FROM {schema}.subscriptions WHERE topic_name = $1"),
                &[topic.as_str().into()],
            )
            .unwrap();
        }
    });
}

fn read_desired() -> Vec<Desired> {
    let Some((_oid, schema)) = crate::installed_extension() else {
        return Vec::new();
    };
    let query = format!(
        "SELECT coalesce(jsonb_agg(jsonb_build_object(\
            'topic', topic_name, \
            'requested_type', requested_type, \
            'idle_seconds', extract(epoch from (statement_timestamp() - keepalive_at))::double precision\
        )), '[]'::jsonb) FROM {schema}.subscriptions"
    );
    let Ok(Some(pgrx::JsonB(value))) = Spi::get_one::<pgrx::JsonB>(&query) else {
        return Vec::new();
    };
    value
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    Some(Desired {
                        topic: row.get("topic")?.as_str()?.to_owned(),
                        requested_type: row
                            .get("requested_type")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        idle_seconds: row
                            .get("idle_seconds")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.0),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn subscribe(
    node: &Node,
    topic: &str,
    requested_type: Option<&str>,
    limit: usize,
) -> Result<Live, String> {
    let message_type = match requested_type {
        Some(kind) if !kind.is_empty() => kind.to_owned(),
        _ => resolve(node, topic)?,
    };
    let kind: MessageTypeName = message_type
        .as_str()
        .try_into()
        .map_err(|error: rclrs::DynamicMessageError| crate::describe(&error))?;
    let mailbox = Arc::new(Mutex::new(Mailbox::default()));
    let callback_mailbox = Arc::clone(&mailbox);
    let callback_topic = topic.to_owned();
    let callback_type = message_type.clone();
    let subscription = node
        .create_dynamic_subscription(
            kind,
            topic.best_effort().keep_last(10),
            move |message, _| {
                let Ok(mut state) = callback_mailbox.lock() else {
                    return;
                };
                state.sequence += 1;
                match json::payload(
                    &callback_topic,
                    &callback_type,
                    state.sequence,
                    &message.view(),
                    limit,
                ) {
                    Ok(payload) => state.pending = Some((state.sequence, payload)),
                    Err(error) => state.error = Some(error),
                }
            },
        )
        .map_err(|error| {
            format!(
                "{} (message packages are resolved from the database server's \
                 AMENT_PREFIX_PATH; start the server with the ROS environment sourced)",
                crate::describe(&error)
            )
        })?;
    Ok(Live {
        _subscription: subscription,
        message_type,
        mailbox,
    })
}

/// Resolve a topic's message type from one graph snapshot.
///
/// This never waits: an unadvertised topic is reported as an error and retried
/// on the next reconcile, so one missing publisher cannot stall every other
/// topic's messages.
fn resolve(node: &Node, topic: &str) -> Result<String, String> {
    let topics = node
        .get_topic_names_and_types()
        .map_err(|error| crate::describe(&error))?;
    match topics.get(topic) {
        Some(types) if types.len() == 1 => Ok(types[0].clone()),
        Some(_) => Err("topic has multiple message types; set requested_type".to_owned()),
        None => Err(format!("topic {topic} has no advertised message type yet")),
    }
}

/// Write every mailbox that has something new into ros2.messages.
fn flush(topics: &HashMap<String, Live>) {
    let mut messages = Vec::new();
    let mut errors = Vec::new();
    for (topic, live) in topics {
        let Ok(mut state) = live.mailbox.lock() else {
            continue;
        };
        if let Some((sequence, payload)) = state.pending.take() {
            match serde_json::from_str::<Value>(&payload) {
                Ok(value) => messages.push((topic.clone(), sequence, value)),
                Err(error) => errors.push((topic.clone(), error.to_string())),
            }
        }
        if let Some(error) = state.error.take() {
            errors.push((topic.clone(), error));
        }
    }
    if messages.is_empty() && errors.is_empty() {
        return;
    }
    BackgroundWorker::transaction(|| {
        let Some((_oid, schema)) = crate::installed_extension() else {
            return;
        };
        for (topic, sequence, value) in messages {
            Spi::run_with_args(
                &format!(
                    "INSERT INTO {schema}.messages (topic_name, sequence, received_at, message) \
                     VALUES ($1, $2, statement_timestamp(), $3) \
                     ON CONFLICT (topic_name) DO UPDATE SET sequence = EXCLUDED.sequence, \
                     received_at = EXCLUDED.received_at, message = EXCLUDED.message"
                ),
                &[
                    topic.as_str().into(),
                    sequence.into(),
                    pgrx::JsonB(value).into(),
                ],
            )
            .unwrap();
            Spi::run_with_args(
                &format!(
                    "UPDATE {schema}.subscriptions \
                     SET last_message_at = statement_timestamp(), last_error = NULL \
                     WHERE topic_name = $1"
                ),
                &[topic.as_str().into()],
            )
            .unwrap();
        }
        for (topic, error) in errors {
            Spi::run_with_args(
                &format!(
                    "UPDATE {schema}.subscriptions SET last_error = $2 WHERE topic_name = $1"
                ),
                &[topic.as_str().into(), error.as_str().into()],
            )
            .unwrap();
        }
    });
}
