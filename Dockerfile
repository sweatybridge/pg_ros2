# Humble's binary packages target Ubuntu 22.04 (Jammy).
FROM ros:humble-ros-base-jammy AS build
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates clang curl gnupg libclang-dev libssl-dev pkg-config \
    ros-humble-example-interfaces ros-humble-test-msgs \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo 'deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt jammy-pgdg main' > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-18 postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home builder
USER builder
ENV PATH="/home/builder/.cargo/bin:/usr/lib/postgresql/18/bin:${PATH}"
RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.96.0 \
    && rustup component add rustfmt clippy \
    && cargo install cargo-pgrx --version =0.19.2 --locked \
    && cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config
WORKDIR /home/builder/pg_ros2
USER root
RUN chown builder:builder /usr/share/postgresql/18/extension /usr/lib/postgresql/18/lib
USER builder
COPY --chown=builder:builder Cargo.toml Cargo.lock pg_ros2.control rust-toolchain.toml build.rs ./
COPY --chown=builder:builder src ./src
RUN source /opt/ros/humble/setup.bash && sha256sum Cargo.lock > /tmp/pg_ros2-lock.sha256 \
    && cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config \
    && sha256sum --check /tmp/pg_ros2-lock.sha256

FROM build AS test
ENV USER=builder
RUN source /opt/ros/humble/setup.bash \
    && cargo fmt --all -- --check \
    && cargo clippy --locked --all-targets --no-default-features --features pg18 -- -D warnings \
    && cargo pgrx test pg18 --release --no-default-features \
    && sha256sum --check /tmp/pg_ros2-lock.sha256

FROM scratch AS package
COPY --from=build /home/builder/pg_ros2/target/release/pg_ros2-pg18/ /

# A development runtime. Start the database as its unprivileged OS owner.
FROM build AS runtime
USER root
RUN install -m 755 target/release/pg_ros2-pg18/usr/lib/postgresql/18/lib/pg_ros2.so /usr/lib/postgresql/18/lib/ \
    && install -m 644 target/release/pg_ros2-pg18/usr/share/postgresql/18/extension/* /usr/share/postgresql/18/extension/ \
    && chown root:root /usr/share/postgresql/18/extension /usr/lib/postgresql/18/lib \
    && install -d -o postgres -g postgres /var/lib/postgresql/pg_ros2
USER postgres
ENV PGDATA=/var/lib/postgresql/pg_ros2
COPY --chmod=755 scripts/start-postgres.sh /usr/local/bin/start-pg-ros2
ENTRYPOINT ["/ros_entrypoint.sh", "/usr/local/bin/start-pg-ros2"]
CMD ["postgres", "-c", "listen_addresses=*", "-c", "shared_preload_libraries=pg_ros2"]
