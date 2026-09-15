#!/usr/bin/env bash
set -euo pipefail
image=${1:-pg-ros2:humble}
container="pg-ros2-smoke-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$container" -e ROS_DOMAIN_ID=73 "$image" >/dev/null
ready=false
for _ in {1..60}; do
    if docker exec -u postgres "$container" pg_isready -q; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    docker logs "$container"
    exit 1
fi
docker exec -u postgres "$container" psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_ros2'
[[ $(docker exec -u postgres "$container" psql -Atc 'SELECT hello_pg_ros2()') == 'Hello, pg_ros2' ]]
[[ $(docker exec -u postgres "$container" psql -Atc "SELECT EXISTS (SELECT FROM ros2_nodes(0) WHERE node_name = 'pg_ros2_' || pg_backend_pid()::text)") == t ]]

# The publisher shares the server's DDS domain and network namespace.
docker exec -d -u postgres "$container" /ros_entrypoint.sh \
    ros2 topic pub /pg_ros2_smoke std_msgs/msg/String '{data: smoke}'
found=false
for _ in {1..10}; do
    if [[ $(docker exec -u postgres "$container" psql -v ON_ERROR_STOP=1 -Atc \
        "SELECT EXISTS (SELECT FROM ros2_topics(1000) WHERE topic_name = '/pg_ros2_smoke' AND message_type = 'std_msgs/msg/String')") == t ]]; then
        found=true
        break
    fi
done
[[ "$found" == true ]]
echo 'Smoke passed: extension, local node, and external ROS topic discovery'
