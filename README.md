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

## Build and run with Docker

Targets Linux amd64, Ubuntu 22.04, PostgreSQL 18, Rust 1.96.0, pgrx 0.19.2, and
rclrs 0.7.0. The runtime image enables `shared_preload_libraries=pg_ros2` by default.

```sh
docker build --target test -t pg-ros2:test .
docker build --target runtime -t pg-ros2:humble .
docker run --rm -d --name pg-ros2 -e ROS_DOMAIN_ID=42 pg-ros2:humble
# Wait for PostgreSQL to report that it is ready before running SQL.
docker exec -u postgres pg-ros2 psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_ros2'
docker exec -u postgres pg-ros2 psql -c 'SELECT * FROM worker_status'
docker exec -u postgres pg-ros2 psql -c 'SELECT * FROM nodes'
bash scripts/smoke.sh pg-ros2:humble
docker stop pg-ros2
```

The runtime is a development image containing build tools. It uses local socket
trust authentication and SCRAM for host connections; no database password is
preconfigured. Data is ephemeral unless a volume is mounted at
`/var/lib/postgresql/pg_ros2` writable by the container's `postgres` user.

Discovery requires a shared `ROS_DOMAIN_ID` and a network with working DDS multicast.
On Linux, `--network host` can support discovery of host ROS nodes. Docker Desktop
may need additional network configuration. No PostgreSQL port needs publishing for
these `docker exec` examples.

See [development and packaging instructions](docs/README.md).
