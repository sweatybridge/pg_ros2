"""Run inside the smoke container as postgres, after sourcing ROS.

Uses system libpq directly so the runtime needs no extra Python database package.
"""
import ctypes as c
import json
import time

import rclpy
from std_msgs.msg import String


class Notify(c.Structure):
    _fields_ = [("channel", c.c_char_p), ("pid", c.c_int), ("payload", c.c_char_p)]


pq = c.CDLL("libpq.so.5")
for name, result, args in [
    ("PQconnectdb", c.c_void_p, [c.c_char_p]),
    ("PQstatus", c.c_int, [c.c_void_p]),
    ("PQexec", c.c_void_p, [c.c_void_p, c.c_char_p]),
    ("PQresultStatus", c.c_int, [c.c_void_p]),
    ("PQresultErrorMessage", c.c_char_p, [c.c_void_p]),
    ("PQntuples", c.c_int, [c.c_void_p]),
    ("PQgetvalue", c.c_char_p, [c.c_void_p, c.c_int, c.c_int]),
    ("PQclear", None, [c.c_void_p]),
    ("PQconsumeInput", c.c_int, [c.c_void_p]),
    ("PQnotifies", c.POINTER(Notify), [c.c_void_p]),
    ("PQfreemem", None, [c.c_void_p]),
    ("PQfinish", None, [c.c_void_p]),
    ("PQsendQuery", c.c_int, [c.c_void_p, c.c_char_p]),
    ("PQisBusy", c.c_int, [c.c_void_p]),
    ("PQgetResult", c.c_void_p, [c.c_void_p]),
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
        return pq.PQgetvalue(result, 0, 0).decode() if pq.PQntuples(result) else None
    finally:
        pq.PQclear(result)


def notifications(conn):
    assert pq.PQconsumeInput(conn) == 1
    messages = []
    while True:
        notification = pq.PQnotifies(conn)
        if not notification:
            return messages
        try:
            assert notification.contents.channel == b'/pg_ros2_messages'
            messages.append(json.loads(notification.contents.payload))
        finally:
            pq.PQfreemem(notification)


def wait(check, publish=None):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if publish:
            publish()
        result = check()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("subscription check timed out")


admin, listener, second_listener, caller = connect(), connect(), connect(), connect()
rclpy.init()
node = rclpy.create_node("pg_ros2_subscription_smoke")
caller_pid = int(sql(caller, "SELECT pg_backend_pid()"))
call = b"CALL ros2.subscribe('/pg_ros2_messages')"


def rejected(statement, expected):
    result = pq.PQexec(caller, statement.encode())
    try:
        assert pq.PQresultStatus(result) == 7, "expected CALL to fail"
        assert expected in pq.PQresultErrorMessage(result).decode()
    finally:
        pq.PQclear(result)


def finished():
    assert pq.PQconsumeInput(caller) == 1
    return pq.PQisBusy(caller) == 0


def cancel():
    assert sql(admin, f"SELECT pg_cancel_backend({caller_pid})") == "t"
    wait(finished)
    result = pq.PQgetResult(caller)
    assert result
    try:
        assert pq.PQresultStatus(result) == 7
        assert b"canceling statement" in pq.PQresultErrorMessage(result)
    finally:
        pq.PQclear(result)
    assert not pq.PQgetResult(caller)
    assert sql(caller, "SELECT 1") == "1", "connection not reusable after cancellation"


try:
    assert sql(admin, "SELECT to_regclass('ros2.subscriptions') IS NULL") == "t"
    for conn in (listener, second_listener):
        sql(conn, 'LISTEN "/pg_ros2_messages"')
    sql(caller, "SET statement_timeout = 0")
    sql(caller, "SET client_connection_check_interval = '1s'")
    sql(caller, "BEGIN")
    rejected(call.decode(), "outside a transaction block")
    sql(caller, "ROLLBACK")
    rejected("CALL ros2.subscribe('relative')", "fully qualified")
    rejected("CALL ros2.subscribe('/' || repeat('x', 63))", "63-byte")
    sql(admin, "CREATE ROLE ros_subscriber_test LOGIN; GRANT USAGE ON SCHEMA ros2 TO ros_subscriber_test")
    sql(caller, "SET ROLE ros_subscriber_test")
    rejected(call.decode(), "permission denied")
    sql(admin, "GRANT EXECUTE ON PROCEDURE ros2.subscribe(text) TO ros_subscriber_test")
    # Start before the publisher exists: the call must wait for discovery.
    assert pq.PQsendQuery(caller, call) == 1
    time.sleep(1)
    assert not finished()
    publisher = node.create_publisher(String, "/pg_ros2_messages", 10)
    text = 'hello "ROS"\n世界'
    publish = lambda: publisher.publish(String(data=text))
    messages = wait(lambda: notifications(listener), publish)
    assert not finished(), "CALL returned before delivering notifications"
    assert messages[0]["message"] == {"data": text}, messages
    assert messages[0]["topic"] == "/pg_ros2_messages"
    assert messages[0]["message_type"] == "std_msgs/msg/String"
    wait(lambda: notifications(second_listener), publish)
    more = wait(lambda: notifications(listener), publish)
    assert more[-1]["sequence"] > messages[0]["sequence"]
    # Drain previous traffic, then verify oversized messages are skipped.
    time.sleep(0.3)
    notifications(listener)
    for _ in range(15):
        publisher.publish(String(data="x" * 8000))
        time.sleep(0.1)
        assert not notifications(listener)
        assert not finished()
    wait(lambda: notifications(listener), publish)
    cancel()
    wait(lambda: publisher.get_subscription_count() == 0)
    notifications(listener)
    for _ in range(10):
        publish()
        time.sleep(0.1)
        assert not notifications(listener), "canceled CALL still delivers"
    # Reusing the same backend must construct a fresh ROS context and subscription.
    assert pq.PQsendQuery(caller, call) == 1
    wait(lambda: notifications(listener), publish)
    cancel()
    wait(lambda: publisher.get_subscription_count() == 0)
    # A durable launch must return immediately and stream from its own backend.
    sql(admin, "SELECT df.grant_usage('ros_subscriber_test')")
    notifications(listener)
    instance = sql(caller, "SELECT df.start($$CALL ros2.subscribe('/pg_ros2_messages')$$)")
    assert instance
    try:
        messages = wait(lambda: notifications(listener), publish)
        assert messages[-1]["message"] == {"data": text}
        assert sql(caller, "SELECT 1") == "1", "df.start kept the submitting backend busy"
    finally:
        sql(caller, f"SELECT df.cancel('{instance}')")
        # Stop any in-flight SQL activity as well as canceling its workflow.
        sql(admin, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
            "WHERE usename = 'ros_subscriber_test' AND pid <> " + str(caller_pid) +
            " AND query = $$CALL ros2.subscribe('/pg_ros2_messages')$$")
    wait(lambda: publisher.get_subscription_count() == 0)
    print("Subscriptions passed: CALL streams before return, topic/channel identity, "
          "JSON, fan-out, repeated messages, payload limit, permissions, "
          "transaction rejection, cancellation, backend reuse, df.start delivery")
finally:
    # Cancellation is safe even when an earlier assertion failed while CALL ran.
    sql(admin, f"SELECT pg_cancel_backend({caller_pid})")
    pq.PQfinish(caller)
    node.destroy_node()
    rclpy.shutdown()
    for conn in (admin, listener, second_listener):
        pq.PQfinish(conn)
