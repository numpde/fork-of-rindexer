#!/usr/bin/env bash
set -euo pipefail
# One local Docker engine and one deployment name own the database volume.
# Neither this host lock nor the fixed container name fences another host.
if [[ $# != 2 || ! "$1" =~ ^(start|stop|status)$ ]]; then
  echo 'Usage: bash containers/runtime/deploy.sh start|stop|status /absolute/path/runtime.env' >&2
  exit 2
fi
operation=$1
env_file=$2
if (( $(id -u) == 0 )); then
  echo 'Run this deployment as the non-root Docker operator.' >&2; exit 2
fi
if [[ "$env_file" != /* ]] || ! test -f "$env_file" || test -L "$env_file" ||
   [[ $(stat -c '%u:%a' "$env_file") != "$(id -u):600" ]]; then
  echo 'The runtime env file must be an absolute regular file owned by you with mode 0600.' >&2; exit 2
fi
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH DOCKER_API_VERSION
unset COMPOSE_FILE COMPOSE_PROJECT_NAME COMPOSE_PROFILES COMPOSE_ENV_FILES
unset RINDEXER_DOCKER_CONTEXT RINDEXER_IMAGE RINDEXER_PROJECT_DIR POSTGRES_PASSWORD RPC_HTTP_URL CHAIN_ID CONTRACT_ADDRESS START_BLOCK
export COMPOSE_DISABLE_ENV_FILE=1
# Image provenance has one owner, shared with the maintained development inputs.
source "$repo_root/containers/development/images.env"
export POSTGRES_IMAGE
lock_file=/tmp/rindexer-postgres-deployment.lock
if test -e "$lock_file" && { ! test -f "$lock_file" || test -L "$lock_file" || [[ $(stat -c '%u:%a' "$lock_file") != "$(id -u):600" ]]; }; then
  echo 'The deployment lock must be a regular mode-0600 file owned by this operator.' >&2; exit 2
fi
umask 077
exec 9>"$lock_file"
flock -n 9 || { echo 'Another deployment operation is running; wait for it to finish.' >&2; exit 2; }
render=(docker compose --env-file "$env_file" --project-name rindexer-postgres
         --project-directory "$repo_root" -f "$repo_root/compose/postgres-runtime.yml")
scratch_dir=$(mktemp -d)
trap 'rm -rf -- "$scratch_dir"' EXIT
# Let Compose parse dotenv and interpolation; do not invent another dotenv parser.
# The rendered file contains credentials and stays private until it is removed.
"${render[@]}" config --format json > "$scratch_dir/config.json"
context=$(python3 "$repo_root/containers/runtime/check.py" "$scratch_dir/config.json")
# Resolve once, before effects. Pin the socket so editing a context cannot redirect
# later commands in this stop-before-start invocation to a different database.
endpoint_json=$(docker context inspect "$context" --format '{{json .Endpoints.docker.Host}}')
endpoint=$(python3 - "$endpoint_json" <<'PYTHON'
import json, sys
endpoint = json.loads(sys.argv[1])
if not isinstance(endpoint, str) or not endpoint.startswith('unix:///') or any(ord(c) < 32 for c in endpoint):
    raise SystemExit('The selected Docker context must use a local Unix socket; no deployment services were changed.')
print(endpoint)
PYTHON
)
compose=(docker --host "$endpoint" "${render[@]:1}")
case "$operation" in
  start)
    # Wait for the old process to exit before either schema startup or new writes.
    "${compose[@]}" stop --timeout 120 indexer
    "${compose[@]}" up -d --no-build --pull never --wait --wait-timeout 90 postgres
    "${compose[@]}" up -d --no-build --pull never --wait --wait-timeout 30 indexer
    echo 'Indexer started. Check http://127.0.0.1:8080/health and each subscription lag before relying on it.'
    ;;
  stop)
    "${compose[@]}" stop --timeout 120 indexer
    "${compose[@]}" stop --timeout 60 postgres
    ;;
  status) "${compose[@]}" ps --all ;;
esac
