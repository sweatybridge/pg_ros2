#!/usr/bin/env bash
# Build pg_ros2 from this checkout and verify it against a real
# PostgreSQL 18 + ROS 2 Humble container.
#
# Two phases, both driven through Docker:
#
#   1. BUILD   - compile the release package in the ROS/pgrx builder image
#                (default: pg-ros2:action-dev, see .bootstrap/actions.Dockerfile).
#   2. RUNTIME - install the freshly built pg_ros2.so into the runtime image
#                BEFORE the server first starts, then assert that the graph
#                worker waits for CREATE EXTENSION instead of crash-looping
#                (the historical InvalidPosition regression), and that it
#                installs a snapshot once the extension exists.
#
# The source tree is copied into the build container with docker cp, so the
# host does not need Docker file sharing for this checkout.
#
# Exit code 0 means every assertion passed; 1 means a check failed or a Docker
# command failed. Containers are always removed, and the artifacts directory is
# removed unless --keep-artifacts is given.
set -euo pipefail

build_image='pg-ros2:action-dev'
runtime_image='ghcr.io/sweatybridge/pg_ros2:latest'
build_container='pg-ros2-verify-build'
runtime_container='pg-ros2-verify-runtime'
artifacts_dir=''
skip_build=false
keep_artifacts=false

usage() {
    cat <<'USAGE'
Usage: bash scripts/verify-docker.sh [options]

  --build-image IMAGE       builder image (default pg-ros2:action-dev)
  --runtime-image IMAGE     runtime image (default ghcr.io/sweatybridge/pg_ros2:latest)
  --build-container NAME    builder container name (default pg-ros2-verify-build)
  --runtime-container NAME  runtime container name (default pg-ros2-verify-runtime)
  --artifacts-dir DIR       artifacts directory (default .verify-artifacts)
  --skip-build              reuse a previously built .so
  --keep-artifacts          keep the artifacts directory
  -h, --help                show this help
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --build-image) build_image="$2"; shift 2 ;;
        --runtime-image) runtime_image="$2"; shift 2 ;;
        --build-container) build_container="$2"; shift 2 ;;
        --runtime-container) runtime_container="$2"; shift 2 ;;
        --artifacts-dir) artifacts_dir="$2"; shift 2 ;;
        --skip-build) skip_build=true; shift ;;
        --keep-artifacts) keep_artifacts=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
if [ -z "$artifacts_dir" ]; then
    artifacts_dir="$repo_root/.verify-artifacts"
fi

pg_major=18
work_dir=/home/builder/pg_ros2
target_dir=/tmp/target
package_out="$target_dir/release/pg_ros2-pg18"

build_script="$(cat <<'BUILD'
set -eo pipefail
source /opt/ros/humble/setup.bash
rm -rf /home/builder/work
mkdir -p /home/builder/work
cp -r /home/builder/pg_ros2/. /home/builder/work/
cd /home/builder/work
echo BUILD_START
cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config --features pg18 --no-default-features
echo BUILD_DONE
BUILD
)"

if [ -t 1 ]; then
    cyan=$'\033[36m'
    reset=$'\033[0m'
else
    cyan=''
    reset=''
fi

stage() {
    printf '\n%s=== %s ===%s\n' "$cyan" "$1" "$reset"
}

# Run docker and fail the script with the combined output on a non-zero exit.
docker_strict() {
    local output status
    output="$(docker "$@" 2>&1)" || {
        status=$?
        printf 'docker %s failed (exit %s)\n%s\n' "$*" "$status" "$output" >&2
        exit 1
    }
    printf '%s' "$output"
}

remove_container() {
    docker rm -f "$1" >/dev/null 2>&1 || true
}

container_log() {
    docker logs "$1" 2>&1 || true
}

build_package() {
    stage "Build release package in $build_image"
    remove_container "$build_container"
    docker_strict create --name "$build_container" --user builder \
        -e "CARGO_TARGET_DIR=$target_dir" -w "$work_dir" \
        "$build_image" bash -lc "$build_script" >/dev/null
    local name
    for name in Cargo.toml Cargo.lock pg_ros2.control; do
        docker_strict cp "$repo_root/$name" "$build_container:$work_dir/$name" >/dev/null
    done
    docker_strict cp "$repo_root/src" "$build_container:$work_dir/src" >/dev/null
    docker_strict start "$build_container" >/dev/null

    local exit_code log
    exit_code="$(docker_strict wait "$build_container")"
    exit_code="$(printf '%s' "$exit_code" | tr -d '[:space:]')"
    log="$(docker_strict logs "$build_container")"
    printf '%s\n' "$log" > "$artifacts_dir/build.log"
    if [ "$exit_code" != 0 ] || ! grep -q 'BUILD_DONE' "$artifacts_dir/build.log"; then
        printf '%s\n' "$log"
        printf 'Build failed (container exit %s); full log in %s\n' "$exit_code" "$artifacts_dir/build.log" >&2
        exit 1
    fi
    printf 'Build succeeded\n'
    docker_strict cp "$build_container:$package_out" "$artifacts_dir" >/dev/null
}

wait_postgres() {
    local name="$1" deadline
    deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if docker exec -u postgres "$name" pg_isready -q >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    printf 'PostgreSQL in %s did not become ready\n' "$name" >&2
    exit 1
}

wait_for_log() {
    local name="$1" pattern="$2" timeout="$3" deadline log
    deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        log="$(container_log "$name")"
        if printf '%s' "$log" | grep -q -- "$pattern"; then
            printf '%s' "$log"
            return 0
        fi
        # Stop early on the failure we are guarding against.
        if printf '%s' "$log" | grep -q -- 'InvalidPosition'; then
            printf '%s' "$log"
            return 0
        fi
        sleep 0.5
    done
    container_log "$name"
}

wait_for_healthy() {
    local name="$1" timeout="$2" deadline row
    deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        row="$(docker exec -u postgres "$name" psql -Atc 'SELECT last_refreshed IS NOT NULL AND last_error IS NULL FROM ros2.worker_status' 2>/dev/null || true)"
        row="$(printf '%s' "$row" | tr -d '[:space:]')"
        if [ "$row" = t ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

passed=true
healthy=false
waiting_has_wait=false
waiting_has_invalid=false
final_has_invalid=false

cleanup() {
    remove_container "$build_container"
    remove_container "$runtime_container"
    if [ "$keep_artifacts" = true ]; then
        printf 'Artifacts kept in %s\n' "$artifacts_dir"
    else
        rm -rf "$artifacts_dir"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if ! command -v docker >/dev/null 2>&1; then
    printf 'docker CLI not found on PATH\n' >&2
    exit 1
fi
mkdir -p "$artifacts_dir"

if [ "$skip_build" = true ]; then
    stage 'Skipping build (--skip-build)'
else
    build_package
fi

so=''
while IFS= read -r candidate; do
    so="$candidate"
    break
done < <(find "$artifacts_dir" -type f -name pg_ros2.so 2>/dev/null)
if [ -z "$so" ]; then
    printf 'Built pg_ros2.so not found under %s; run without --skip-build first\n' "$artifacts_dir" >&2
    exit 1
fi

stage 'Runtime: install built .so before the server first starts'
remove_container "$runtime_container"
docker_strict create --name "$runtime_container" --ipc=shareable -e ROS_DOMAIN_ID=73 "$runtime_image" >/dev/null
docker_strict cp "$so" "$runtime_container:/usr/lib/postgresql/$pg_major/lib/pg_ros2.so" >/dev/null
docker_strict start "$runtime_container" >/dev/null
wait_postgres "$runtime_container"

stage 'Assert: worker waits for CREATE EXTENSION instead of crashing'
waiting_log="$(wait_for_log "$runtime_container" waiting_for_extension 30)"
if printf '%s' "$waiting_log" | grep -q 'waiting_for_extension'; then waiting_has_wait=true; fi
if printf '%s' "$waiting_log" | grep -q 'InvalidPosition'; then waiting_has_invalid=true; fi

stage 'Create the extension'
docker_strict exec -u postgres "$runtime_container" psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_ros2' >/dev/null

stage 'Assert: worker installs a snapshot'
if wait_for_healthy "$runtime_container" 30; then healthy=true; fi
final_log="$(container_log "$runtime_container")"
if printf '%s' "$final_log" | grep -q 'InvalidPosition'; then final_has_invalid=true; fi
printf '%s\n' "$final_log" > "$artifacts_dir/runtime.log"

if [ "$waiting_has_wait" = true ]; then s1=PASS; else s1=FAIL; fi
if [ "$waiting_has_invalid" = true ]; then s2=FAIL; else s2=PASS; fi
if [ "$healthy" = true ]; then s3=PASS; else s3=FAIL; fi
if [ "$final_has_invalid" = true ]; then s4=FAIL; else s4=PASS; fi

stage 'Results'
printf 'waiting_for_extension logged : %s\n' "$s1"
printf 'no InvalidPosition (waiting) : %s\n' "$s2"
printf 'worker healthy after install : %s\n' "$s3"
printf 'no InvalidPosition (final)   : %s\n' "$s4"

if [ "$s1" != PASS ] || [ "$s2" != PASS ] || [ "$s3" != PASS ] || [ "$s4" != PASS ]; then
    passed=false
    printf '\n--- runtime log ---\n'
    printf '%s\n' "$final_log"
fi

if [ "$passed" = true ]; then
    stage 'VERIFY PASSED'
else
    stage 'VERIFY FAILED'
fi

if [ "$passed" = true ]; then exit 0; else exit 1; fi
