#!/usr/bin/env bash
# Run after cargo pgrx test/install, as an unprivileged user in sourced Humble.
set -euo pipefail
pg_config=${1:-$(cargo pgrx info pg-config pg18)}
export PATH="$($pg_config --bindir):$PATH"
test_dir=$(mktemp -d /tmp/pg-ros2-actions.XXXXXX)
export PGHOST="$test_dir"
export PGPORT=${PG_ROS2_TEST_PORT:-28819}
export PGUSER=$(id -un)
export ROS_DOMAIN_ID=${PG_ROS2_TEST_DOMAIN:-74}
export ROS_LOG_DIR="$test_dir/ros-logs"
cleanup() {
    status=$?
    if [[ $status != 0 ]]; then cat "$test_dir/postgres.log" >&2 || true; fi
    pg_ctl -D "$test_dir/data" -m immediate stop >/dev/null 2>&1 || true
    rm -rf -- "$test_dir"
    exit "$status"
}
trap cleanup EXIT
initdb -D "$test_dir/data" --auth=trust >/dev/null
pg_ctl -D "$test_dir/data" -l "$test_dir/postgres.log" \
    -o "-k $test_dir -p $PGPORT -c listen_addresses='' -c shared_preload_libraries=pg_ros2 -c pg_ros2.database=graph_test" start
createdb graph_test
psql -d graph_test -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA ros_graph; CREATE EXTENSION pg_ros2 SCHEMA ros_graph'
python3 "$(dirname "$0")/actions-smoke.py"
python3 "$(dirname "$0")/parameters-smoke.py"
