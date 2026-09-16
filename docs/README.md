# Development and packaging

## Native Humble environment

Use Ubuntu 22.04 with [ROS 2 Humble installed](https://docs.ros.org/en/humble/Installation/Ubuntu-Install-Debs.html)
and PostgreSQL 18 plus `postgresql-server-dev-18` from the PostgreSQL apt repository.
The Dockerfile records all required apt packages, including `libclang-dev`,
`libssl-dev`, `ros-humble-example-interfaces`, and `ros-humble-test-msgs`.

```sh
source /opt/ros/humble/setup.bash
rustup toolchain install 1.96.0 --profile minimal --component rustfmt --component clippy
cargo install cargo-pgrx --version =0.19.2 --locked
cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config
cargo fmt --all -- --check
cargo clippy --locked --all-targets --no-default-features --features pg18 -- -D warnings
cargo pgrx test pg18 --release --no-default-features
cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config
```

Run pgrx tests as an unprivileged OS user with write access to this **development**
PostgreSQL installation's extension and library directories. The Docker test stage
sets this up. Do not change ownership of a production installation for tests.
The test suite checks atomic snapshot reconciliation, duplicate node names, multiple
message types, unchanged snapshots, and persisted discovery errors. `scripts/smoke.sh`
checks worker startup, automatic graph arrivals/removals, SQL failure rollback and
recovery, reader privileges, extension reinstallation, and PostgreSQL restart.
It also runs `subscriptions-smoke.py` to exercise ROS message delivery to two SQL
listeners, JSON escaping, repeated messages, oversized payloads, delivery before
CALL returns, cancellation, backend reuse, permissions, and rejection of atomic calls.
The smoke image also preloads pg_durable, installs it in the test database, and
checks notification delivery from `df.start('CALL ...')` under a trusted login role
while the submitting connection remains available.
The pgrx tests cover
dynamic JSON conversion of nested messages, arrays, sequences, and byte limits.
They also cover all ROS parameter value types and parameter snapshot reconciliation.
`scripts/parameters-smoke.py` checks initial parameter discovery, updates, declarations,
removals, service timeouts with snapshot preservation, recovery, and node removal.

Use the release profile for tests and packages. rclrs 0.7.0 vendors some interfaces
from newer ROS distributions (for example `SetLoggerLevelsResult`), whose native
symbols do not exist in Humble. Release LTO removes these unused bindings from this
extension. An unoptimized pgrx test build retains them and fails to load.
Adding new message APIs requires checking their Humble compatibility explicitly.

### Subscription benchmarks

Run `cargo pgrx bench pg18 --no-default-features` in the sourced Humble environment.
The CI benchmark job runs this command with the default release profile.
Two benchmarks measure subscription message processing for 256-byte and 4096-byte
`std_msgs/msg/String` values: dynamic JSON encoding, the bounded queue transfer,
and the same SPI `pg_notify` helper used by `subscribe`. Message construction and
queue setup are outside the timing loop. Each iteration rolls back a subtransaction
to discard pending notifications and prevent state accumulation between samples.

These are per-message microbenchmarks, not end-to-end delivery measurements. They
exclude DDS discovery/reception, executor polling, transaction commits, and client
notification delivery. `CALL subscribe` cannot run directly inside the benchmark
runner's transaction because it requires a top-level, non-atomic call.

For a server installed separately, start PostgreSQL from an environment which has
sourced Humble's `setup.bash`. That environment supplies `LD_LIBRARY_PATH`,
`AMENT_PREFIX_PATH`, `ROS_DISTRO`, and any chosen `ROS_DOMAIN_ID` or
`RMW_IMPLEMENTATION`. Set a writable `ROS_LOG_DIR` if the server user's home is not
writable. A systemd service does not inherit your interactive shell's environment;
use a service wrapper that sources the ROS setup before executing PostgreSQL.
Add `pg_ros2` to `shared_preload_libraries`, set `pg_ros2.database` to the target
database (default `postgres`), and restart the server. The Docker runtime enables
preloading in its default command. Allow one slot in `max_worker_processes`.

## Export an installable package

```sh
docker build --target package --output type=local,dest=target/package .
find target/package -type f
```

The exported tree contains `/usr/lib/postgresql/18/lib/pg_ros2.so` and the control
and versioned SQL files beneath `/usr/share/postgresql/18/extension`. Copy that tree
into the same paths on a matching Ubuntu 22.04 amd64 / PostgreSQL 18 host with the
same Humble runtime packages installed. The archive does not bundle ROS libraries
and is not portable to other PostgreSQL majors or arbitrary Linux distributions.
After installation, run `CREATE EXTENSION pg_ros2` and the README queries.

## Execution model and limits

ROS initialization occurs after fork, in the graph worker or a backend executing
`CALL subscribe`. The postmaster only registers the worker and configuration.
Graph-table reads do not initialize ROS. The graph worker owns one persistent node
and executor, spins in bounded 100 ms intervals, and uses an atomic flag to record
graph changes. Its SPI calls execute on the PostgreSQL worker thread.

`CALL subscribe(topic)` creates a separate ROS node in the calling backend after
fork. It validates a non-atomic CALL context and opens SPI with `SPI_OPT_NONATOMIC`.
Message callbacks only enqueue bounded JSON payloads. The main backend thread spins
in 100 ms intervals, checks PostgreSQL interrupts, drains up to 256 payloads, and
commits `pg_notify` calls through the outer SPI connection. Nested pgrx SPI clients
close before each commit; only owned Rust data survives transaction boundaries.
Canceling the call releases its ROS resources. SQL errors abort the current batch;
previously committed notifications remain delivered. Native ROS calls may delay
cancellation. No background worker or persistent subscription registry is involved.
Native DDS/rclrs reception and dynamic field views allocate before the JSON bound.

Both graph queries run outside database transactions. A complete, changed snapshot
is written with typed SQL parameters in one short transaction. A failed read retains
the last snapshot and records the error. SQL errors abort the transaction and exit
the worker; the postmaster restarts it after five seconds. Lock waits are bounded
by a two-second timeout. PostgreSQL signals are checked between executor spins;
native ROS setup, graph calls, and teardown can still delay shutdown.

The worker connects as the bootstrap superuser and writes only in the extension
schema resolved from `pg_extension`. ROS peers and native runtime libraries are
part of the server trust boundary. Keep write/DDL access to the extension tables
restricted. Ordinary readers require only schema USAGE and table SELECT privileges.
The graph tables are derived caches and are repopulated after server restart.

Parameter polling uses the same observer and executor, outside SQL transactions.
Requests run concurrently across nodes, with a three-second shared deadline per
stage (service readiness, listing, retrieval). The worker polls again five seconds
after completion, independent of parameter events. Parameter snapshots and their
status are persisted in a separate transaction so an unreachable parameter service
does not prevent graph discovery. This is a full-domain snapshot: one failed peer
preserves all prior parameter rows. Nodes without advertised parameter list services
are excluded, and duplicate fully qualified node names are queried once.

## CI and releases

The Package workflow runs when `Cargo.toml` changes on `main`, or when dispatched
manually. It builds PostgreSQL 18 Debian packages for amd64 and arm64 and uploads
them to a GitHub Release tagged `v<version>` using the version from `Cargo.toml`.
The Package workflow calls Publish Docker Image after creating the release.
Images are published to `ghcr.io/sweatybridge/pg_ros2` using `GITHUB_TOKEN`, with
`packages: write` permission; no Docker Hub credentials are required. Version and
`latest` tags include both amd64 and arm64, with architecture-specific tags also
available. Publish Docker Image can also be dispatched manually with a released
version (without the leading `v`).
