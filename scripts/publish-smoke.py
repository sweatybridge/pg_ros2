"""Run inside the smoke container as postgres, after sourcing ROS.

Checks ros2.publish end to end: inferred and explicit message types, nested
messages, sequences, one-shot delivery to a live subscriber, JSON validation,
strict NULL handling, and execution permissions. Only std_msgs types are used so
the runtime image needs no test packages. Uses system libpq directly so the
runtime needs no extra Python database package.
"""
import ctypes as c
import json
import time

import rclpy
from std_msgs.msg import Float64MultiArray, String


pq = c.CDLL("libpq.so.5")
for name, result, args in [
    ("PQconnectdb", c.c_void_p, [c.c_char_p]),
    ("PQstatus", c.c_int, [c.c_void_p]),
    ("PQexec", c.c_void_p, [c.c_void_p, c.c_char_p]),
    ("PQresultStatus", c.c_int, [c.c_void_p]),
    ("PQresultErrorMessage", c.c_char_p, [c.c_void_p]),
    ("PQntuples", c.c_int, [c.c_void_p]),
    ("PQgetvalue", c.c_char_p, [c.c_void_p, c.c_int, c.c_int]),
    ("PQgetisnull", c.c_int, [c.c_void_p, c.c_int, c.c_int]),
    ("PQclear", None, [c.c_void_p]),
    ("PQfinish", None, [c.c_void_p]),
]:
    function = getattr(pq, name)
    function.restype = result
    function.argtypes = args


def connect():
    conn = pq.PQconnectdb(b"dbname=graph_test user=postgres")
    assert conn and pq.PQstatus(conn) == 0, "libpq connection failed"
    return conn


def sql(conn, statement):
    result = pq.PQexec(conn, statement.encode())
    assert result, "PQexec failed"
    try:
        assert pq.PQresultStatus(result) in (1, 2), pq.PQresultErrorMessage(result)
        if pq.PQntuples(result) and not pq.PQgetisnull(result, 0, 0):
            return pq.PQgetvalue(result, 0, 0).decode()
        return None
    finally:
        pq.PQclear(result)


def fails(conn, statement, expected):
    result = pq.PQexec(conn, statement.encode())
    try:
        assert pq.PQresultStatus(result) == 7, "expected " + statement + " to fail"
        assert expected in pq.PQresultErrorMessage(result).decode(), (
            expected,
            pq.PQresultErrorMessage(result).decode(),
        )
    finally:
        pq.PQclear(result)


admin, caller = connect(), connect()
rclpy.init()
node = rclpy.create_node("pg_ros2_publish_smoke")
received = {"strings": [], "arrays": []}


def spin_until(predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rclpy.spin_once(node, timeout=0.1)
        if predicate():
            return True
    return False


string_subscription = node.create_subscription(
    String, "/pg_ros2_publish", lambda message: received["strings"].append(message.data), 10
)
array_subscription = node.create_subscription(
    Float64MultiArray, "/pg_ros2_array", lambda message: received["arrays"].append(message), 10
)
# Give DDS discovery a head start before the first publish waits for a match.
rclpy.spin_once(node, timeout_sec=0.5)

try:
    # The inferred overload resolves the unique advertised type and delivers.
    assert (
        sql(caller, "SELECT ros2.publish('/pg_ros2_publish', '{\"data\":\"hello\"}'::jsonb)")
        == "std_msgs/msg/String"
    )
    assert spin_until(lambda: received["strings"]), "inferred publish was not delivered"
    assert received["strings"][0] == "hello", received["strings"]

    # The explicit overload also delivers to the same topic.
    received["strings"].clear()
    assert (
        sql(
            caller,
            "SELECT ros2.publish('/pg_ros2_publish', 'std_msgs/msg/String', "
            "'{\"data\":\"typed\"}'::jsonb)",
        )
        == "std_msgs/msg/String"
    )
    assert spin_until(lambda: received["strings"]), "explicit publish was not delivered"
    assert received["strings"][0] == "typed", received["strings"]

    # The explicit overload works without any graph advertisement.
    assert (
        sql(
            caller,
            "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', "
            "'{\"data\":\"void\"}'::jsonb)",
        )
        == "std_msgs/msg/String"
    )

    # Nested messages, a sequence of messages, and a float sequence are decoded.
    payload = json.dumps(
        {
            "layout": {"dim": [{"label": "row", "size": 3, "stride": 3}], "data_offset": 7},
            "data": [0.5, 1.5, 2.5],
        }
    )
    assert (
        sql(
            caller,
            "SELECT ros2.publish('/pg_ros2_array', 'std_msgs/msg/Float64MultiArray', '"
            + payload
            + "'::jsonb)",
        )
        == "std_msgs/msg/Float64MultiArray"
    )
    assert spin_until(lambda: received["arrays"]), "typed publish was not delivered"
    message = received["arrays"][0]
    assert message.layout.data_offset == 7
    assert len(message.layout.dim) == 1
    assert message.layout.dim[0].label == "row"
    assert message.layout.dim[0].size == 3
    assert message.layout.dim[0].stride == 3
    assert list(message.data) == [0.5, 1.5, 2.5]

    # STRICT returns NULL instead of publishing a null argument.
    assert sql(caller, "SELECT ros2.publish(NULL, '{\"data\":\"x\"}'::jsonb)") is None

    # Invalid names, fields, shapes, and types are rejected.
    fails(
        caller,
        "SELECT ros2.publish('relative', '{\"data\":\"x\"}'::jsonb)",
        "fully qualified",
    )
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_missing', '{\"data\":\"x\"}'::jsonb)",
        "no unique advertised message type",
    )
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', '{\"nope\":1}'::jsonb)",
        "unknown message field",
    )
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', '{\"data\":1}'::jsonb)",
        "expected a JSON string",
    )
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', '[]'::jsonb)",
        "must be a JSON object",
    )
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_void', 'no_such_pkg/msg/Thing', '{}'::jsonb)",
        "no_such_pkg",
    )

    # Execution is restricted to trusted roles.
    sql(admin, "DROP ROLE IF EXISTS ros_publisher_test")
    sql(admin, "CREATE ROLE ros_publisher_test LOGIN")
    sql(admin, "GRANT USAGE ON SCHEMA ros2 TO ros_publisher_test")
    sql(caller, "SET ROLE ros_publisher_test")
    fails(
        caller,
        "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', '{\"data\":\"x\"}'::jsonb)",
        "permission denied",
    )
    sql(admin, "GRANT EXECUTE ON FUNCTION ros2.publish(text, text, jsonb) TO ros_publisher_test")
    assert (
        sql(
            caller,
            "SELECT ros2.publish('/pg_ros2_void', 'std_msgs/msg/String', "
            "'{\"data\":\"granted\"}'::jsonb)",
        )
        == "std_msgs/msg/String"
    )
    print(
        "Publish passed: inferred and explicit types, nested messages, sequences, "
        "one-shot delivery, strict NULL, validation, permissions"
    )
finally:
    node.destroy_node()
    rclpy.shutdown()
    for conn in (admin, caller):
        pq.PQfinish(conn)
