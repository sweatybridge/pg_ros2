#!/usr/bin/env bash
set -euo pipefail
if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
    initdb --auth-local=trust --auth-host=scram-sha-256
fi
exec "$@"
