"""Run as postgres in a sourced ROS environment, against graph_test/ros_graph."""
import json
import subprocess
import threading
import time

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.parameter import Parameter


def sql(statement):
    return subprocess.check_output(
        ["psql", "-X", "-d", "graph_test", "-v", "ON_ERROR_STOP=1", "-Atc",
         "SET search_path=ros_graph,pg_catalog; " + statement],
        text=True, timeout=10,
    ).strip().removeprefix("SET\n")


def wait(check):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise AssertionError("parameter check timed out: " + sql("TABLE parameter_status"))


def values():
    return json.loads(sql(
        "SELECT coalesce(jsonb_object_agg(parameter_name, value), '{}'::jsonb) "
        "FROM parameters WHERE node_name='parameter_probe' AND namespace='/nested'"
    ))


rclpy.init()
node = rclpy.create_node("parameter_probe", namespace="/nested")
disabled = rclpy.create_node("no_parameter_services", start_parameter_services=False)
executor = SingleThreadedExecutor()
executor.add_node(node)
executor.add_node(disabled)
thread = threading.Thread(target=executor.spin, daemon=True)
blocked = None
expected = {
    "flag": True, "count": 9223372036854775807, "gain": 1.25,
    "nested.greeting": "quote'\\\"\nhello", "flags": [True, False],
    "counts": [-9223372036854775808, 12], "gains": [1.5, 2.5],
    "strings": ["a", "b"], "bytes": [0, 255],
}
try:
    for name, value in expected.items():
        node.declare_parameter(name, [bytes([item]) for item in value] if name == "bytes" else value)
    thread.start()
    wait(lambda: all(values().get(name) == value for name, value in expected.items()))
    assert sql("SELECT count(*) FROM parameters WHERE node_name='no_parameter_services'") == "0"
    node.set_parameters([Parameter("count", value=42)])
    node.declare_parameter("new_value", "arrived")
    node.undeclare_parameter("nested.greeting")
    wait(lambda: values().get("count") == 42 and values().get("new_value") == "arrived"
         and "nested.greeting" not in values())
    # A discovered service that never spins must time out, preserving saved rows.
    before = values()
    blocked = rclpy.create_node("unresponsive_parameters")
    wait(lambda: sql("SELECT last_error IS NOT NULL FROM parameter_status") == "t")
    assert values() == before
    assert sql("SELECT count(*) FROM nodes WHERE node_name='unresponsive_parameters'") == "1"
    blocked.destroy_node()
    blocked = None
    wait(lambda: sql("SELECT last_error IS NULL FROM parameter_status") == "t")
    executor.remove_node(node)
    node.destroy_node()
    node = None
    wait(lambda: values() == {})
    print("Parameters smoke passed: all value types, initial values, updates, additions, removals, timeout preservation, recovery, node removal")
finally:
    executor.shutdown()
    if thread.is_alive():
        thread.join(timeout=5)
    if blocked is not None:
        blocked.destroy_node()
    if node is not None:
        node.destroy_node()
    disabled.destroy_node()
    rclpy.shutdown()
