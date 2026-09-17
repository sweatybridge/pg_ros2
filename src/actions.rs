//! Durable intent reconciled by the existing observer worker. ROS calls always
//! happen outside SQL transactions; callbacks never call PostgreSQL.
mod transport;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::JsonB;
use rclrs::Node;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use transport::{Event, Transport};

extension_sql_file!("actions.sql", name = "action_tables");

const RETRY: Duration = Duration::from_secs(5);
const RECONNECT: Duration = Duration::from_secs(30);
const MAX_ACTIVE: usize = 64;

struct Live {
    transport: Transport,
    reconnect_at: Instant,
    result_at: Instant,
    cancel_at: Instant,
    awaiting_acceptance: bool,
}

#[derive(Default)]
pub(crate) struct Bridge {
    extension: Option<pg_sys::Oid>,
    active: HashMap<String, Live>,
    retry: HashMap<String, Instant>,
}

impl Bridge {
    pub(crate) fn tick(&mut self, node: &Node) {
        let snapshot = BackgroundWorker::transaction(|| {
            let (oid, schema) = Spi::get_two::<pg_sys::Oid, String>(
                "SELECT e.oid, quote_ident(n.nspname) FROM pg_extension e \
                 JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='pg_ros2'",
            )
            .unwrap();
            let (Some(oid), Some(schema)) = (oid, schema) else {
                return None;
            };
            let rows = Spi::get_one::<JsonB>(&format!(
                "SELECT coalesce(jsonb_agg(to_jsonb(g)), '[]'::jsonb) FROM \
                 (SELECT goal_id, action_name, action_type, goal, desired_state, dispatch_state, cancel_response \
                  FROM {schema}.action_goals WHERE completed_at IS NULL \
                  ORDER BY created_at, goal_id LIMIT {MAX_ACTIVE}) g"
            )).unwrap().unwrap().0;
            Some((oid, schema, rows))
        });
        let Some((oid, schema, rows)) = snapshot else {
            self.active.clear();
            self.retry.clear();
            self.extension = None;
            return;
        };
        if self.extension != Some(oid) {
            self.active.clear();
            self.retry.clear();
            self.extension = Some(oid);
        }
        let rows = rows.as_array().expect("SQL array");
        let ids: HashSet<&str> = rows
            .iter()
            .map(|r| r["goal_id"].as_str().unwrap())
            .collect();
        self.active.retain(|id, _| ids.contains(id.as_str()));
        self.retry.retain(|id, _| ids.contains(id.as_str()));
        for row in rows {
            let id = row["goal_id"].as_str().unwrap();
            if self.retry.get(id).is_some_and(|at| Instant::now() < *at) {
                continue;
            }
            if let Err(error) = self.advance(node, &schema, row) {
                update(&schema, id, "last_error = $2", Some(&error), None);
                self.active.remove(id);
                self.retry.insert(id.to_owned(), Instant::now() + RETRY);
                pgrx::warning!(
                    "pg_ros2 event=action_transport_failed goal_id={} error={}",
                    id,
                    error
                );
            }
        }
    }

    fn advance(&mut self, node: &Node, schema: &str, row: &Value) -> Result<(), String> {
        let id = row["goal_id"].as_str().unwrap();
        let cancel = row["desired_state"] == "cancel";
        let pending = row["dispatch_state"] == "pending";
        // Cancellation before dispatch is resolved locally, without contacting ROS.
        if pending && cancel {
            update(
                schema,
                id,
                "dispatch_state = 'canceled', completed_at = statement_timestamp()",
                None,
                None,
            );
            self.active.remove(id);
            return Ok(());
        }
        if row["action_type"] != "example_interfaces/action/Fibonacci" {
            return Err("unsupported action type in action_goals".into());
        }
        // Recreate clients periodically to recover lost replies/server restarts.
        // This drops their pending requests, keeping client-side memory bounded.
        if self
            .active
            .get(id)
            .is_some_and(|live| Instant::now() >= live.reconnect_at)
        {
            self.active.remove(id);
        }
        if !self.active.contains_key(id) {
            self.active.insert(
                id.to_owned(),
                Live {
                    transport: Transport::new(
                        node,
                        row["action_name"].as_str().unwrap(),
                        uuid_bytes(id)?,
                    )?,
                    reconnect_at: Instant::now() + RECONNECT,
                    result_at: Instant::now(),
                    cancel_at: Instant::now(),
                    awaiting_acceptance: false,
                },
            );
        }
        let live = self.active.get_mut(id).unwrap();
        for event in live.transport.poll()? {
            match event {
                Event::Accepted(true) => {
                    live.awaiting_acceptance = false;
                    live.reconnect_at = Instant::now() + RECONNECT;
                    update(
                        schema,
                        id,
                        "dispatch_state = 'accepted', last_error = NULL",
                        None,
                        None,
                    );
                    status(schema, id, 1);
                }
                Event::Accepted(false) => {
                    update(schema, id, "dispatch_state = 'rejected', completed_at = statement_timestamp(), last_error = NULL", None, None);
                    self.active.remove(id);
                    return Ok(());
                }
                Event::Status(code) => status(schema, id, code),
                Event::Feedback(value) => update(schema, id, "feedback = $3", None, Some(value)),
                Event::Cancel(code, contains_goal) => {
                    let response = match (code, contains_goal) {
                        (0, true) => "accepted",
                        (1, _) => "rejected",
                        (2, _) => "unknown",
                        (3, _) => "terminated",
                        _ => "unconfirmed",
                    };
                    update(schema, id, "cancel_response = $2", Some(response), None);
                }
                Event::Result(code @ 4..=6, result) => {
                    update(
                        schema,
                        id,
                        "observed_state = $2, result = $3, completed_at = statement_timestamp(), \
                        dispatch_state = 'accepted', last_error = NULL",
                        state(code),
                        Some(result),
                    );
                    self.active.remove(id);
                    return Ok(());
                }
                Event::Result(_, _) => {
                    update(
                        schema,
                        id,
                        "last_error = $2",
                        Some("server cannot establish the result; goal will not be resubmitted"),
                        None,
                    );
                    live.result_at = Instant::now() + RETRY;
                }
            }
        }
        if !live.transport.ready()? {
            return Ok(());
        }
        if pending {
            // Commit intent BEFORE the external side effect. After a crash an
            // uncertain row is reconciled, never automatically sent again.
            let claimed = BackgroundWorker::transaction(|| {
                Spi::get_one_with_args::<bool>(&format!(
                    "WITH claimed AS (UPDATE {schema}.action_goals SET dispatch_state='uncertain', \
                     updated_at=statement_timestamp() WHERE goal_id=$1::uuid AND dispatch_state='pending' \
                     AND desired_state='run' AND completed_at IS NULL RETURNING 1) \
                     SELECT EXISTS (SELECT FROM claimed)"
                ), &[id.into()]).unwrap().unwrap()
            });
            if claimed {
                live.awaiting_acceptance = true;
                // A lost SendGoal response must not block reconciliation forever.
                live.reconnect_at = Instant::now() + RETRY;
                live.transport.send(&row["goal"])?;
            }
            return Ok(());
        }
        if !live.awaiting_acceptance && Instant::now() >= live.result_at {
            live.transport.request_result()?;
        }
        if cancel && row["cancel_response"] != "rejected" && Instant::now() >= live.cancel_at {
            live.transport.cancel()?;
            live.cancel_at = Instant::now() + RETRY;
        }
        Ok(())
    }
}

fn update(schema: &str, id: &str, assignments: &str, text: Option<&str>, json: Option<Value>) {
    BackgroundWorker::transaction(|| {
        // Always bind all three parameters, even for an update that does not use
        // a value, so SQL text never incorporates ROS/user payloads.
        Spi::run_with_args(
            &format!(
                "UPDATE {schema}.action_goals SET {assignments}, updated_at=statement_timestamp() \
             WHERE goal_id=$1::uuid AND completed_at IS NULL AND ($2::text IS NULL OR true) \
             AND ($3::jsonb IS NULL OR true)"
            ),
            &[id.into(), text.into(), json.map(JsonB).into()],
        )
        .unwrap();
    });
}

fn state(code: i8) -> Option<&'static str> {
    match code {
        1 => Some("accepted"),
        2 => Some("executing"),
        3 => Some("canceling"),
        4 => Some("succeeded"),
        5 => Some("canceled"),
        6 => Some("aborted"),
        _ => None,
    }
}

fn status(schema: &str, id: &str, code: i8) {
    let Some(value) = state(code) else {
        return;
    };
    BackgroundWorker::transaction(|| {
        Spi::run_with_args(&format!(
            "UPDATE {schema}.action_goals SET observed_state=$2, dispatch_state='accepted', \
             updated_at=statement_timestamp() WHERE goal_id=$1::uuid AND completed_at IS NULL \
             AND array_position(ARRAY['unknown','accepted','executing','canceling','succeeded','canceled','aborted'], observed_state) \
                 < array_position(ARRAY['unknown','accepted','executing','canceling','succeeded','canceled','aborted'], $2) \
             AND observed_state NOT IN ('succeeded','canceled','aborted')"
        ), &[id.into(), value.into()]).unwrap();
    });
}

fn uuid_bytes(id: &str) -> Result<[u8; 16], String> {
    let hex = id.replace('-', "");
    if hex.len() != 32 || !hex.is_ascii() {
        return Err("invalid goal UUID".into());
    }
    let mut bytes = [0; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| e.to_string())?;
    }
    if bytes == [0; 16] {
        return Err("zero UUID is reserved by ROS cancellation".into());
    }
    Ok(bytes)
}
