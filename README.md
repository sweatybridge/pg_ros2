# pg_ros2

A [pgrx](https://github.com/pgcentralfoundation/pgrx) extension wrapping
[ros2_rust / rclrs](https://github.com/ros2-rust/ros2_rust) for **ROS 2 Humble Hawksbill**.
A PostgreSQL background worker keeps ROS graph snapshots in ordinary tables.

## Setup

Add these settings to `postgresql.conf` and restart PostgreSQL:

```conf
shared_preload_libraries = 'pg_ros2'  # append to any existing libraries
pg_ros2.database = 'postgres'
```

Install the extension in the configured database:

```sql
CREATE EXTENSION pg_ros2;
SELECT * FROM nodes;
SELECT * FROM topics;
SELECT * FROM worker_status;
```

One worker monitors the server's configured ROS domain in one database. It starts
after recovery, creates one persistent ROS observer, and begins discovery. It waits
for `CREATE EXTENSION` to commit if the extension has not been installed yet.
The tables start empty and are populated asynchronously after an initial one-second
warm-up. Installing into a custom schema is supported. Changing `pg_ros2.database`
requires a server restart; the database must exist before the worker can connect.

| Table | Columns |
| --- | --- |
| `nodes` | `node_name`, `namespace`, `refreshed_at` |
| `topics` | `topic_name`, `message_type`, `refreshed_at` |
| `worker_status` | `singleton`, `worker_pid`, `last_checked`, `last_refreshed`, `last_error` |

Graph notifications trigger a new snapshot. rclrs also checks once per second to
cover discovery races. Changed snapshots replace both tables in one transaction;
unchanged snapshots leave the graph tables untouched. Readers can continue reading
the previous committed snapshot during refresh. Discovery or SQL failures preserve
that snapshot. Empty successful discovery removes previously observed entities.

The observer is excluded from `nodes`. Duplicate node names are retained; `topics`
has one row per topic/type pair. Graph discovery is eventually consistent and does
not establish application health. The two graph queries happen sequentially, so a
rapidly changing graph may differ between them even though the database update is atomic.

`refreshed_at` and `last_refreshed` record the last saved snapshot, not a heartbeat.
`last_checked` records the latest discovery attempt, including when the graph is
unchanged. Check that it is recent and `last_error` is null before relying on a
snapshot. An empty status table means the worker has not reached it yet.
ROS discovery errors are recorded in `last_error`; SQL errors and startup failures
are logged by PostgreSQL and can leave `last_checked` stale. The postmaster retries
failed workers after five seconds. Inspect `pg_stat_activity` for backend type
`pg_ros2 graph worker` and PostgreSQL logs for details.

Readers need no ROS access. Grant access as appropriate:

```sql
GRANT SELECT ON nodes, topics, worker_status TO reader_role;
-- Also grant USAGE on the extension schema when needed.
```

The worker owns synchronization; direct table edits are unsupported. The previous
`ros2_nodes()`, `ros2_topics()`, `refresh_nodes()`, and `refresh_topics()` SQL functions
have been removed. Queries now read the saved tables and never create ROS nodes.
This development version changes the installation SQL; existing installations need
a fresh extension installation. No versioned upgrade script is provided yet.

## Subscribe to ROS messages with LISTEN / NOTIFY

In a listening connection, commit `LISTEN` before starting the subscriber:

```sql
LISTEN "/chatter";
```

The Docker image includes `pg_durable` and preloads both extensions. Install
`pg_durable` in the same database, then grant a trusted login role permission to
launch subscriptions (run these setup statements as the administrator):

```sql
CREATE EXTENSION pg_durable;
CREATE ROLE ros_subscriber LOGIN;
GRANT USAGE ON SCHEMA public TO ros_subscriber;
GRANT EXECUTE ON PROCEDURE public.subscribe(text) TO ros_subscriber;
SELECT df.grant_usage('ros_subscriber');
```

Connect as `ros_subscriber`, or use `SET ROLE ros_subscriber` from an administrator
session, and launch the subscription:

```sql
SELECT df.start($$CALL public.subscribe('/chatter')$$, 'ROS /chatter');
-- Save the returned instance ID for monitoring and cancellation.
SELECT df.status('<instance_id>');
```

`df.start` returns while the procedure runs in a separate backend. Commit the
launch before expecting messages. Submit only the `CALL` as the workflow step;
do not wrap it in `BEGIN`/`COMMIT`. Use the actual extension schema in place of
`public` if installed elsewhere. Superuser workflow submission stays disabled;
the login role above has the required privileges. For a custom database, set
both `pg_ros2.database` and `pg_durable.database` in the server command.

To stop a subscription, cancel its workflow as the submitting role:

```sql
SELECT df.cancel('<instance_id>');
```

An administrator can also cancel any still-running SQL activity after canceling
the workflow:

```sql
SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE usename = 'ros_subscriber'
  AND query = $$CALL public.subscribe('/chatter')$$;
```

Workflow persistence does not persist ROS messages: an interrupted subscription
starts a fresh ROS session when re-executed, and missed notifications have no replay.
Each active subscription occupies one execution connection.

Alternatively, run the procedure directly in a separate, dedicated connection:

```sql
SET statement_timeout = 0;
SET client_connection_check_interval = '1s';
CALL subscribe('/chatter');
```

`CALL` runs indefinitely until canceled or an error occurs. Run it in your client's
background task or in a separate terminal. Use autocommit: it cannot run inside
`BEGIN`/`COMMIT`, a function, or another atomic context. A procedure is required
because each notification batch must commit while the call is still running.
Cancel with your client's query-cancel operation, Ctrl+C in `psql`, or
`pg_cancel_backend(pid)`. Restart the call after a server restart or failure.
The connection-check setting lets PostgreSQL detect a disconnected client during
an otherwise idle subscription on supported platforms.

The topic name is exactly the PostgreSQL channel name. Use a fully qualified ROS
name and quote it in `LISTEN`. Names over PostgreSQL's 63-byte channel limit are
rejected rather than truncated. There is no subscription table or channel mapping.
Run one `CALL` per topic; multiple calls for the same topic each send notifications.
Any number of SQL sessions can listen on the channel. `UNLISTEN "/chatter"` stops
only that listening session; cancel the `CALL` to stop receiving from ROS.

The procedure waits for the topic to appear, discovers its message type, and creates
one ROS subscription. Multiple advertised types cause an error. The selected type
stays fixed for the call's lifetime. Its C introspection and type-support libraries
must be installed in the database server's sourced ROS environment. This subscription
runs independently of the graph worker and does not require shared preloading.

Each notification contains JSON:

```json
{"topic":"/chatter","message_type":"std_msgs/msg/String","sequence":0,"message":{"data":"hello"}}
```

The sequence increases per call, preventing PostgreSQL from folding identical
messages within one transaction. Scalars, strings, nested messages, arrays, and
sequences are supported. Non-finite floats become JSON `null`; long-double fields
are unsupported. Use a client that consumes asynchronous notifications. In `psql`,
execute a query to display pending notifications; keep listeners out of long-running
transactions.

Delivery is transient with no replay. ROS QoS is best-effort, volatile, depth 10.
The callback queue holds 256 messages and drops new messages when full. JSON payloads
over 7,999 bytes (including the envelope), unsupported fields, or nesting over 64
levels are dropped. Drop counts and encoding errors are reported to the calling
connection as warnings, at most once per second. ROS or SQL errors terminate the
call; notifications from earlier committed batches remain delivered.

The procedure loads native ROS libraries, so execution is restricted by default.
Grant it only to trusted roles:

```sql
GRANT EXECUTE ON PROCEDURE subscribe(text) TO ros_subscriber;
-- Also grant USAGE on the extension schema when needed.
```

PostgreSQL notification channels have **no per-channel access controls**: any user
in the database can listen or send spoofed notifications. Use a trusted database
for sensitive topics; restricting procedure execution does not restrict listening.

## Build and run with Docker

Targets Linux amd64 and arm64, Ubuntu 22.04, PostgreSQL 18, Rust 1.96.0, pgrx 0.19.2, and
rclrs 0.7.0. The runtime image installs the pg_durable 0.2.8 PostgreSQL 18
Debian release package from `sweatybridge/pg_durable` for the target architecture
and enables `shared_preload_libraries=pg_ros2,pg_durable` by default. Published
multi-architecture manifests include both amd64 and arm64 images.

```sh
docker build --build-arg PG_ROS2_VERSION=<released-version> -t pg-ros2:humble .
docker run --rm -d --name pg-ros2 -e ROS_DOMAIN_ID=42 pg-ros2:humble
# Wait for PostgreSQL to report that it is ready before running SQL.
docker exec -u postgres pg-ros2 psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_ros2'
docker exec -u postgres pg-ros2 psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_durable'
docker exec -u postgres pg-ros2 psql -c 'SELECT * FROM worker_status'
docker exec -u postgres pg-ros2 psql -c 'SELECT * FROM nodes'
bash scripts/smoke.sh pg-ros2:humble
docker stop pg-ros2
```

The runtime installs released packages for both extensions. It uses local socket
trust authentication and SCRAM for host connections; no database password is
preconfigured. Data is ephemeral unless a volume is mounted at
`/var/lib/postgresql/pg_ros2` writable by the container's `postgres` user.

Discovery requires a shared `ROS_DOMAIN_ID` and a network with working DDS multicast.
On Linux, `--network host` can support discovery of host ROS nodes. Docker Desktop
may need additional network configuration. No PostgreSQL port needs publishing for
these `docker exec` examples.

See [development and packaging instructions](docs/README.md).
