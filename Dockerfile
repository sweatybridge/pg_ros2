FROM ros:humble-ros-base-jammy

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo 'deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt jammy-pgdg main' > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-18 \
    && rm -rf /var/lib/apt/lists/*

ARG PG_ROS2_VERSION
ARG TARGETARCH

ADD https://github.com/sweatybridge/pg_durable/releases/download/v0.2.8/pg-durable-postgresql-18_0.2.8-1_${TARGETARCH}.deb /tmp/pg_durable.deb
ADD https://github.com/sweatybridge/pg_ros2/releases/download/v${PG_ROS2_VERSION}/pg-ros2-pg18_${PG_ROS2_VERSION}-1.jammy_${TARGETARCH}.deb /tmp/pg_ros2.deb

RUN apt-get update \
  && apt-get install -y --no-install-recommends /tmp/pg_ros2.deb /tmp/pg_durable.deb \
  && rm -rf /var/lib/apt/lists/* /tmp/pg_ros2.deb /tmp/pg_durable.deb

ENV PATH=/usr/lib/postgresql/18/bin:$PATH
# pg_durable's internal connections use the existing local socket trust rule.
ENV PGHOST=/var/run/postgresql
USER postgres
ENV PGDATA=/var/lib/postgresql/pg_ros2
COPY --chmod=755 scripts/start-postgres.sh /usr/local/bin/start-pg-ros2
ENTRYPOINT ["/ros_entrypoint.sh", "/usr/local/bin/start-pg-ros2"]
CMD ["postgres", "-c", "listen_addresses=*", "-c", "shared_preload_libraries=pg_ros2,pg_durable"]
