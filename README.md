# pg_ros2

A minimal [pgrx](https://github.com/pgcentralfoundation/pgrx) extension wrapping
[ros2_rust / rclrs](https://github.com/ros2-rust/ros2_rust) for **ROS 2 Humble Hawksbill**.
It exposes ROS graph discovery as PostgreSQL tables.

```sql
CREATE EXTENSION pg_ros2;
SELECT * FROM ros2_nodes();
SELECT * FROM ros2_topics(1000);
```

| Function | Result |
| --- | --- |
| `hello_pg_ros2()` | `Hello, pg_ros2` (installation check) |
| `ros2_nodes(discovery_ms integer DEFAULT 250)` | `node_name text, namespace text` |
| `ros2_topics(discovery_ms integer DEFAULT 250)` | `topic_name text, message_type text` |

Discovery requires a PostgreSQL superuser. Each call creates a temporary node named
`pg_ros2_<backend_pid>` in `/`, waits 0–2000 ms, reads the graph, and releases its ROS
resources. Node results include that temporary node. Topic results contain one row
per topic/type pair. Discovery is eventually consistent: an empty or incomplete
snapshot does not prove that no remote entities exist. Increase the wait if needed.

This first implementation provides graph queries only; publishers, subscriptions,
services, and background workers are outside its scope.

## Build and run with Docker

Targets Linux amd64, Ubuntu 22.04, PostgreSQL 18, Rust 1.96.0, pgrx 0.19.2, and
rclrs 0.6.0. The pinned rclrs release includes its required Rust message bindings;
this graph-only wrapper needs no separate colcon workspace or generated application
messages. Its native ROS libraries are installed in the image.

```sh
docker build --target test -t pg-ros2:test .
docker build --target runtime -t pg-ros2:humble .
docker run --rm -d --name pg-ros2 -e ROS_DOMAIN_ID=42 pg-ros2:humble
docker exec -u postgres pg-ros2 psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_ros2'
docker exec -u postgres pg-ros2 psql -c 'SELECT * FROM ros2_nodes()'
docker stop pg-ros2
```

Wait for `database system is ready to accept connections` in `docker logs pg-ros2`
before executing SQL. The runtime image is a development image containing build
tools. It uses local socket trust authentication and SCRAM for host connections;
no database password is preconfigured. Data is ephemeral unless you mount a volume
at `/var/lib/postgresql/pg_ros2` writable by the image's `postgres` user.

For discovery across containers, use the same `ROS_DOMAIN_ID` and a shared network
with working DDS multicast. On Linux, `--network host` is often convenient for
discovery of host ROS nodes. Docker Desktop may need additional network configuration.
No PostgreSQL port needs to be published for the `docker exec` examples.

See [build, test, and package instructions](docs/README.md) for native Linux usage.
