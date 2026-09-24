//! Typed adapters for ROS action protocols. Using the generated services lets
//! the bridge supply its durable UUID (rclrs request_goal generates a fresh one).
use rclrs::vendor::{
    action_msgs::{msg::GoalStatusArray, srv::*},
    example_interfaces::action::*,
    unique_identifier_msgs::msg::UUID,
};
use rclrs::{Client, IntoPrimitiveOptions, Node, Promise, Subscription};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

pub(super) enum Event {
    Accepted(bool),
    Status(i8),
    Feedback(Value),
    Result(i8, Value),
    Cancel(i8, bool),
}

pub(super) struct Transport {
    uuid: UUID,
    send: Client<Fibonacci_SendGoal>,
    result: Client<Fibonacci_GetResult>,
    cancel: Client<CancelGoal>,
    send_pending: Option<Promise<Fibonacci_SendGoal_Response>>,
    result_pending: Option<Promise<Fibonacci_GetResult_Response>>,
    cancel_pending: Option<Promise<CancelGoal_Response>>,
    // Coalesce telemetry to one value per goal, rather than an unbounded queue.
    telemetry: Arc<Mutex<(Option<i8>, Option<Value>)>>,
    _feedback: Subscription<Fibonacci_FeedbackMessage>,
    _status: Subscription<GoalStatusArray>,
}

impl Transport {
    pub(super) fn new(node: &Node, name: &str, uuid: [u8; 16]) -> Result<Self, String> {
        let telemetry = Arc::new(Mutex::new((None, None)));
        let feedback_slot = Arc::clone(&telemetry);
        let status_slot = Arc::clone(&telemetry);
        let feedback = node
            .create_subscription(
                format!("{name}/_action/feedback").as_str().keep_last(1),
                move |message: Fibonacci_FeedbackMessage| {
                    if message.goal_id.uuid == uuid {
                        // Bound persisted telemetry. Full native reception still allocates.
                        if message.feedback.sequence.len() <= 16384 {
                            feedback_slot.lock().unwrap().1 =
                                Some(json!({"sequence": message.feedback.sequence}));
                        }
                    }
                },
            )
            .map_err(|e| e.to_string())?;
        let status = node
            .create_subscription(
                format!("{name}/_action/status")
                    .as_str()
                    .transient_local()
                    .keep_last(1),
                move |message: GoalStatusArray| {
                    for status in message.status_list {
                        if status.goal_info.goal_id.uuid == uuid {
                            status_slot.lock().unwrap().0 = Some(status.status);
                        }
                    }
                },
            )
            .map_err(|e| e.to_string())?;
        Ok(Self {
            uuid: UUID { uuid },
            send: node
                .create_client::<Fibonacci_SendGoal>(format!("{name}/_action/send_goal").as_str())
                .map_err(|e| e.to_string())?,
            result: node
                .create_client::<Fibonacci_GetResult>(format!("{name}/_action/get_result").as_str())
                .map_err(|e| e.to_string())?,
            cancel: node
                .create_client::<CancelGoal>(format!("{name}/_action/cancel_goal").as_str())
                .map_err(|e| e.to_string())?,
            send_pending: None,
            result_pending: None,
            cancel_pending: None,
            telemetry,
            _feedback: feedback,
            _status: status,
        })
    }

    pub(super) fn ready(&self) -> Result<bool, String> {
        Ok(self.send.service_is_ready().map_err(|e| e.to_string())?
            && self.result.service_is_ready().map_err(|e| e.to_string())?
            && self.cancel.service_is_ready().map_err(|e| e.to_string())?)
    }

    pub(super) fn send(&mut self, goal: &Value) -> Result<(), String> {
        let order = goal
            .get("order")
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or("invalid Fibonacci goal")?;
        self.send_pending = Some(
            self.send
                .call(&Fibonacci_SendGoal_Request {
                    goal_id: self.uuid.clone(),
                    goal: Fibonacci_Goal { order },
                })
                .map_err(|e| e.to_string())?,
        );
        Ok(())
    }

    pub(super) fn request_result(&mut self) -> Result<(), String> {
        if self.result_pending.is_none() {
            self.result_pending = Some(
                self.result
                    .call(&Fibonacci_GetResult_Request {
                        goal_id: self.uuid.clone(),
                    })
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok(())
    }

    pub(super) fn cancel(&mut self) -> Result<(), String> {
        if self.cancel_pending.is_none() {
            self.cancel_pending = Some(
                self.cancel
                    .call(&CancelGoal_Request {
                        goal_info: rclrs::vendor::action_msgs::msg::GoalInfo {
                            goal_id: self.uuid.clone(),
                            stamp: Default::default(),
                        },
                    })
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok(())
    }

    pub(super) fn poll(&mut self) -> Result<Vec<Event>, String> {
        let mut events = Vec::new();
        if let Some(response) = take(&mut self.send_pending)? {
            events.push(Event::Accepted(response.accepted));
        }
        let (status, feedback) = &mut *self.telemetry.lock().unwrap();
        if let Some(status) = status.take() {
            events.push(Event::Status(status));
        }
        if let Some(feedback) = feedback.take() {
            events.push(Event::Feedback(feedback));
        }
        if let Some(response) = take(&mut self.cancel_pending)? {
            events.push(Event::Cancel(
                response.return_code,
                response
                    .goals_canceling
                    .iter()
                    .any(|g| g.goal_id == self.uuid),
            ));
        }
        if let Some(response) = take(&mut self.result_pending)? {
            events.push(Event::Result(
                response.status,
                json!({"sequence": response.result.sequence}),
            ));
        }
        Ok(events)
    }
}

fn take<T>(pending: &mut Option<Promise<T>>) -> Result<Option<T>, String> {
    let Some(promise) = pending else {
        return Ok(None);
    };
    let value = promise.try_recv().map_err(|e| e.to_string())?;
    if value.is_some() {
        *pending = None;
    }
    Ok(value)
}
