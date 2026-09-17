#!/usr/bin/env bash
set -euo pipefail
image=${1:-pg-ros2:humble}
container="pg-ros2-smoke-$$"
publisher="${container}-publisher"
extra="${container}-extra"
cleanup() {
    if [[ $? != 0 ]]; then
        docker logs "$container" >&2 || true
        docker logs "$publisher" >&2 || true
        sql 'TABLE worker_status; TABLE nodes; TABLE topics' >&2 || true
    fi
    docker rm -f "$extra" "$publisher" "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
# Test a non-default database and schema, including startup before installation.
docker run -d --name "$container" --ipc=shareable -e ROS_DOMAIN_ID=73 "$image" \
    postgres -c shared_preload_libraries=pg_ros2,pg_durable \
    -c pg_ros2.database=graph_test -c pg_durable.database=graph_test >/dev/null
for _ in {1..60}; do
    if docker exec -u postgres "$container" pg_isready -q; then break; fi
    sleep 0.5
done
docker exec -u postgres "$container" psql -v ON_ERROR_STOP=1 -c 'CREATE DATABASE graph_test'
sql() {
    docker exec -u postgres -e PGOPTIONS=-csearch_path=ros_graph,pg_catalog "$container" \
        psql -d graph_test -v ON_ERROR_STOP=1 -Atc "$1"
}
wait_sql() {
    for _ in {1..60}; do
        if [[ $(sql "$1") == t ]]; then return 0; fi
        sleep 0.5
    done
    echo "Timed out: $1" >&2
    return 1
}
sql 'CREATE SCHEMA ros_graph; CREATE EXTENSION pg_ros2 SCHEMA ros_graph' >/dev/null
sql 'CREATE EXTENSION pg_durable' >/dev/null
wait_sql 'SELECT EXISTS (SELECT FROM worker_status WHERE last_refreshed IS NOT NULL AND last_error IS NULL)'
[[ $(sql "SELECT count(*) = 0 FROM nodes WHERE node_name LIKE 'pg_ros2_worker_%'") == t ]]
[[ $(sql "SELECT to_regprocedure('ros_graph.ros2_nodes(integer)') IS NULL AND to_regprocedure('ros_graph.refresh_nodes(integer)') IS NULL") == t ]]
# Share IPC as well as networking so Fast DDS shared-memory transport works.
docker run -d --name "$publisher" --network "container:$container" --ipc "container:$container" --user postgres \
    -e ROS_DOMAIN_ID=73 --entrypoint /ros_entrypoint.sh "$image" \
    ros2 topic pub /pg_ros2_smoke std_msgs/msg/String '{data: smoke}' >/dev/null
wait_sql "SELECT EXISTS (SELECT FROM topics WHERE topic_name = '/pg_ros2_smoke' AND message_type = 'std_msgs/msg/String') AND EXISTS (SELECT FROM nodes)"
docker exec -i -u postgres "$container" /ros_entrypoint.sh python3 - < "$(dirname "$0")/subscriptions-smoke.py"
docker exec -i -u postgres "$container" /ros_entrypoint.sh python3 - < "$(dirname "$0")/actions-smoke.py"
docker exec -i -u postgres "$container" /ros_entrypoint.sh python3 - < "$(dirname "$0")/parameters-smoke.py"
# A blocked write must roll back both tables and be retried after worker restart.
sql "ALTER TABLE topics ADD CONSTRAINT reject_test_topic CHECK (topic_name <> '/pg_ros2_added')" >/dev/null
docker run -d --name "$extra" --network "container:$container" --ipc "container:$container" --user postgres \
    -e ROS_DOMAIN_ID=73 --entrypoint /ros_entrypoint.sh "$image" \
    ros2 topic pub /pg_ros2_added std_msgs/msg/String '{data: extra}' >/dev/null
failed=false
for _ in {1..60}; do
    if docker logs "$container" 2>&1 | grep -q 'violates check constraint "reject_test_topic"'; then
        failed=true
        break
    fi
    sleep 0.5
done
[[ "$failed" == true ]]
# A node can become visible before its topic. Check that both tables still match
# the last committed snapshot timestamp, rather than assuming one graph event.
[[ $(sql 'SELECT bool_and(n.refreshed_at = s.last_refreshed) FROM nodes n CROSS JOIN worker_status s') == t ]]
[[ $(sql 'SELECT bool_and(t.refreshed_at = s.last_refreshed) FROM topics t CROSS JOIN worker_status s') == t ]]
[[ $(sql "SELECT NOT EXISTS (SELECT FROM topics WHERE topic_name = '/pg_ros2_added')") == t ]]
sql 'ALTER TABLE topics DROP CONSTRAINT reject_test_topic' >/dev/null
wait_sql "SELECT EXISTS (SELECT FROM topics WHERE topic_name = '/pg_ros2_added') AND (SELECT last_error IS NULL FROM worker_status)"
# Readers need only SELECT, and reads never create ROS nodes.
sql 'CREATE ROLE graph_reader; GRANT USAGE ON SCHEMA ros_graph TO graph_reader; GRANT SELECT ON nodes, topics, parameters, worker_status, parameter_status TO graph_reader' >/dev/null
sql 'SET ROLE graph_reader; SELECT count(*) FROM nodes; SELECT count(*) FROM topics; SELECT count(*) FROM parameters' >/dev/null
# Removing external publishers must remove their graph records automatically.
docker stop -t 5 "$extra" "$publisher" >/dev/null
wait_sql "SELECT NOT EXISTS (SELECT FROM topics WHERE topic_name IN ('/pg_ros2_smoke', '/pg_ros2_added')) AND NOT EXISTS (SELECT FROM nodes)"
# Reinstall without a graph change must still initialize the new tables.
sql 'DROP EXTENSION pg_ros2; CREATE EXTENSION pg_ros2 SCHEMA ros_graph' >/dev/null
wait_sql 'SELECT EXISTS (SELECT FROM worker_status WHERE last_refreshed IS NOT NULL)'
# A PostgreSQL restart must initialize the saved snapshot again.
sql "INSERT INTO nodes VALUES ('stale_before_restart', '/', now())" >/dev/null
docker restart -t 5 "$container" >/dev/null
for _ in {1..60}; do
    if docker exec -u postgres "$container" pg_isready -q; then break; fi
    sleep 0.5
done
wait_sql "SELECT NOT EXISTS (SELECT FROM nodes WHERE node_name = 'stale_before_restart') AND EXISTS (SELECT FROM worker_status WHERE last_error IS NULL)"
echo 'Smoke passed: automatic startup, graph additions/removals, topic notifications, atomic rollback, worker recovery, reader access, reinstall, and server restart'
