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
The test suite checks SQL installation, local node discovery, repeated context
creation/cleanup, topic queries, wait bounds, and privilege enforcement.

Use the release profile for tests and packages. rclrs 0.6.0 vendors some interfaces
from newer ROS distributions (for example `SetLoggerLevelsResult`), whose native
symbols do not exist in Humble. Release LTO removes these unused bindings from this
graph-only extension. An unoptimized pgrx test build retains them and fails to load.
Adding new message APIs requires checking their Humble compatibility explicitly.

For a server installed separately, start PostgreSQL from an environment which has
sourced Humble's `setup.bash`. That environment supplies `LD_LIBRARY_PATH`,
`AMENT_PREFIX_PATH`, `ROS_DISTRO`, and any chosen `ROS_DOMAIN_ID` or
`RMW_IMPLEMENTATION`. Set a writable `ROS_LOG_DIR` if the server user's home is not
writable. A systemd service does not inherit your interactive shell's environment;
use a service wrapper that sources the ROS setup before executing PostgreSQL.
Do not add this extension to `shared_preload_libraries`.

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

ROS initialization occurs in the calling backend, never in the postmaster. ROS
resources are local to each call. No Rust ROS callbacks invoke PostgreSQL APIs.
Graph updates come from DDS middleware threads, so no executor spin is required.
The discovery delay checks PostgreSQL interrupts every 10 ms; native ROS setup,
graph calls, and teardown may still delay cancellation. The 2000 ms bound applies
to the discovery wait, not total execution time.

Graph functions are `VOLATILE`, `PARALLEL UNSAFE`, and strict for SQL NULL inputs.
ROS errors become SQL errors after resources are released. Each query joins the
configured DDS domain; transaction rollback cannot undo that external activity.
Only superusers can invoke graph discovery, even if EXECUTE privileges are granted.
Treat ROS peers and runtime library paths as part of the server's trust boundary.

## CI and releases

CI builds the Docker test stage. Packaging also starts a disposable server and
checks `CREATE EXTENSION` and both graph functions before uploading the Jammy/PG18
archive and SHA256. A pushed `v<version>` tag runs the same checks and publishes
those artifacts as a GitHub Release after matching the tag to Cargo.toml.
No image registry publishing is configured.
