"""Run as postgres in sourced Humble against graph_test/ros_graph.

The server counts executions to detect duplicate dispatch after worker restart.
"""
import json
import subprocess
import threading
import time

import rclpy
from example_interfaces.action import Fibonacci
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor


def sql(statement):
    return subprocess.check_output(
        ["psql", "-X", "-d", "graph_test", "-v", "ON_ERROR_STOP=1", "-qAtc",
         "SET search_path=ros_graph,pg_catalog; " + statement],
        text=True, timeout=10,
    ).strip().removeprefix("SET\n")


def wait(check, timeout=45):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("action check timed out: " + sql("TABLE action_goals"))


def send(order, name="/pg_ros2_action"):
    return sql(f"SELECT send_goal('{name}', 'example_interfaces/action/Fibonacci', "
               f"'{{\"order\":{order}}}'::jsonb)")


def row(goal):
    return json.loads(sql(f"SELECT to_jsonb(g) FROM action_goals g WHERE goal_id='{goal}'"))


def done(goal):
    wait(lambda: row(goal)["completed_at"] is not None)
    return row(goal)


def rejected(statement):
    command = subprocess.run(
        ["psql", "-X", "-d", "graph_test", "-v", "ON_ERROR_STOP=1", "-Atc",
         "SET search_path=ros_graph,pg_catalog; " + statement],
        capture_output=True, text=True, timeout=10,
    )
    assert command.returncode != 0, command.stdout


executions = []
requests = []


def accept(request):
    requests.append(request.order)
    return GoalResponse.REJECT if request.order < 0 else GoalResponse.ACCEPT


def execute(handle):
    executions.append(bytes(handle.goal_id.uuid).hex())
    sequence = [0, 1]
    for _ in range(handle.request.order):
        if handle.is_cancel_requested:
            handle.canceled()
            return Fibonacci.Result(sequence=sequence)
        sequence.append(sequence[-1] + sequence[-2])
        handle.publish_feedback(Fibonacci.Feedback(sequence=sequence))
        time.sleep(0.15)
    if handle.request.order == 3:
        handle.abort()
    else:
        handle.succeed()
    return Fibonacci.Result(sequence=sequence)


rclpy.init()
node = rclpy.create_node("action_probe")
blocked = rclpy.create_node("action_unresponsive_parameters")
server = ActionServer(
    node, Fibonacci, "/pg_ros2_action", execute,
    goal_callback=accept,
    cancel_callback=lambda handle: CancelResponse.REJECT if handle.request.order == 12 else CancelResponse.ACCEPT,
    callback_group=ReentrantCallbackGroup(), result_timeout=120,
)
executor = MultiThreadedExecutor(num_threads=8)
executor.add_node(node)
thread = threading.Thread(target=executor.spin, daemon=True)
thread.start()
try:
    # Submission rolls back with the caller's transaction.
    count = sql("SELECT count(*) FROM action_goals")
    sql("BEGIN; SELECT send_goal('/pg_ros2_action', 'example_interfaces/action/Fibonacci', '{\"order\":1}'); ROLLBACK")
    assert sql("SELECT count(*) FROM action_goals") == count
    for goal in ("null", "{}", '{"order":null}', '{"order":1.5}', '{"order":2147483648}', '{"order":1,"extra":2}'):
        rejected(f"SELECT send_goal('/pg_ros2_action', 'example_interfaces/action/Fibonacci', '{goal}')")
    rejected("SELECT send_goal('relative', 'example_interfaces/action/Fibonacci', '{\"order\":1}')")
    rejected("SELECT send_goal('/pg_ros2_action', 'unknown/action/Unsupported', '{\"order\":1}')")
    assert sql("SELECT count(*) FROM action_goals") == count

    # Cancellation works even when the goal is beyond the bridge's active window.
    sql("SELECT send_goal('/pg_ros2_absent', 'example_interfaces/action/Fibonacci', '{\"order\":1}') "
        "FROM generate_series(1, 64)")
    queued = send(1, "/pg_ros2_absent")
    assert sql(f"SELECT cancel_goal('{queued}')") == "t"
    assert done(queued)["dispatch_state"] == "canceled"
    sql("SELECT cancel_goal(goal_id) FROM action_goals WHERE action_name='/pg_ros2_absent'")

    # Locally canceled pending goals must never reach the action server.
    before = len(requests)
    local = sql("WITH new_goal AS (SELECT send_goal('/pg_ros2_action', 'example_interfaces/action/Fibonacci', "
                "'{\"order\":2}') AS id) SELECT id FROM new_goal WHERE cancel_goal(id)")
    assert done(local)["dispatch_state"] == "canceled"
    assert len(requests) == before

    success = send(8)
    wait(lambda: row(success)["feedback"] is not None)
    result = done(success)
    assert result["observed_state"] == "succeeded" and result["result"]["sequence"][-1] == 34
    assert sql(f"SELECT cancel_goal('{success}')") == "f"
    assert done(send(-1))["dispatch_state"] == "rejected"
    assert done(send(3))["observed_state"] == "aborted"

    # An unresponsive parameter peer must not delay cancellation of active goals.
    first, second = send(30), send(30)
    wait(lambda: row(first)["observed_state"] == "executing" and row(second)["feedback"] is not None)
    wait(lambda: sql("SELECT last_error IS NOT NULL FROM parameter_status") == "t")
    start = time.monotonic()
    assert sql(f"SELECT cancel_goal('{first}')") == "t"
    assert done(first)["observed_state"] == "canceled"
    assert time.monotonic() - start < 3
    assert done(second)["observed_state"] == "succeeded"

    rejected_cancel = send(12)
    wait(lambda: row(rejected_cancel)["observed_state"] == "executing")
    sql(f"SELECT cancel_goal('{rejected_cancel}')")
    result = done(rejected_cancel)
    # Humble rclpy may return code 0 with an empty goals_canceling list when its
    # user callback rejects cancellation. That must not be treated as accepted.
    assert result["cancel_response"] in ("rejected", "unconfirmed")
    assert result["observed_state"] == "succeeded"

    # Force a worker write failure; clean SIGTERM deliberately does not restart
    # a static PostgreSQL background worker. The remote goal keeps running.
    recovered = send(30)
    wait(lambda: row(recovered)["feedback"] is not None)
    pid = sql("SELECT worker_pid FROM worker_status")
    sql(f"ALTER TABLE action_goals ADD CONSTRAINT action_test_failure "
        f"CHECK (goal_id <> '{recovered}'::uuid) NOT VALID")
    wait(lambda: sql(f"SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid={pid})") == "t")
    sql("ALTER TABLE action_goals DROP CONSTRAINT action_test_failure")
    assert done(recovered)["observed_state"] == "succeeded"
    assert executions.count(recovered.replace("-", "")) == 1

    # Simulate a crash after durable send intent, with no known remote goal.
    unknown = sql("INSERT INTO action_goals(action_name, action_type, goal, dispatch_state) "
                  "VALUES ('/pg_ros2_action', 'example_interfaces/action/Fibonacci', "
                  "'{\"order\":7}', 'uncertain') RETURNING goal_id")
    before = len(requests)
    wait(lambda: row(unknown)["last_error"] is not None)
    assert row(unknown)["completed_at"] is None and row(unknown)["dispatch_state"] == "uncertain"
    assert len(requests) == before
    sql(f"SELECT cancel_goal('{unknown}')")
    wait(lambda: row(unknown)["cancel_response"] == "unknown")
    assert row(unknown)["observed_state"] == "unknown"
    # Remove only the synthetic test fixture, otherwise it deliberately stays unresolved.
    sql(f"DELETE FROM action_goals WHERE goal_id='{unknown}'")

    # Calls are restricted by default; ordinary readers cannot command the robot.
    sql("CREATE ROLE action_reader; GRANT USAGE ON SCHEMA ros_graph TO action_reader; "
        "GRANT SELECT ON action_goals TO action_reader")
    assert sql("SELECT has_function_privilege('action_reader', 'ros_graph.send_goal(text,text,jsonb)', 'EXECUTE')") == "f"
    assert sql("SELECT has_function_privilege('action_reader', 'ros_graph.cancel_goal(uuid)', 'EXECUTE')") == "f"
    assert sql("SELECT has_table_privilege('action_reader', 'action_goals', 'UPDATE')") == "f"
    rejected("SET ROLE action_reader; SELECT send_goal('/pg_ros2_action', 'example_interfaces/action/Fibonacci', '{\"order\":1}')")
    sql("GRANT EXECUTE ON FUNCTION send_goal(text,text,jsonb), cancel_goal(uuid) TO action_reader")
    allowed = sql("SET ROLE action_reader; WITH g AS (SELECT send_goal('/pg_ros2_absent', "
                  "'example_interfaces/action/Fibonacci', '{\"order\":1}') id) "
                  "SELECT id FROM g WHERE cancel_goal(id)")
    assert done(allowed)["dispatch_state"] == "canceled"
    sql("DROP OWNED BY action_reader; DROP ROLE action_reader")
    print("Actions smoke passed: rollback, pre-dispatch cancel, feedback/results, rejection, abort, "
          "concurrent goals, cancellation/rejection, parameter isolation, worker recovery without duplicate dispatch, "
          "unknown outcome reconciliation, reader privileges")
finally:
    executor.shutdown()
    thread.join(timeout=5)
    server.destroy()
    blocked.destroy_node()
    node.destroy_node()
    rclpy.shutdown()
