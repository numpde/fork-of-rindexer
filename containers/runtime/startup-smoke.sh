#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$repo_root"
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH
unset COMPOSE_FILE COMPOSE_PROJECT_NAME COMPOSE_PROFILES COMPOSE_ENV_FILES
source containers/development/images.env
export POSTGRES_IMAGE
export ANVIL_IMAGE=$(cat core/src/indexer/fixtures/anvil-image.txt)
export RINDEXER_IMAGE=${1:?supply the immutable local image ID}
bash containers/runtime/smoke.sh "$RINDEXER_IMAGE"
export RINDEXER_PROJECT_DIR=$PWD/containers/runtime/project
export POSTGRES_PASSWORD=$(printf 'a%.0s' {1..64})
export RPC_HTTP_URL=http://anvil:8545 CHAIN_ID=31337
export CONTRACT_ADDRESS=0x1111111111111111111111111111111111111111 START_BLOCK=0
export COMPOSE_DISABLE_ENV_FILE=1
scratch=$(mktemp -d)
# A unique fixture project and container name never reuse the deployment volume.
project="rindexer-runtime-smoke-$$"
cat > "$scratch/fixture.yml" <<'YAML'
services:
  indexer:
    container_name: ${SMOKE_CONTAINER_NAME:?}
    ports: !override
      - '127.0.0.1::8080'
  anvil:
    image: ${ANVIL_IMAGE:?}
    pull_policy: never
    user: '65532:65532'
    read_only: true
    pids_limit: 128
    mem_limit: 512m
    cpus: 1
    cap_drop: [ALL]
    security_opt: [no-new-privileges:true]
    entrypoint: [/usr/local/bin/anvil]
    command: [--host, 0.0.0.0, --chain-id, '31337', --silent]
    networks: [rpc]
    healthcheck:
      test: [CMD, /usr/local/bin/cast, rpc, --rpc-url, 'http://127.0.0.1:8545', eth_chainId]
      interval: 2s
      timeout: 2s
      retries: 30
YAML
export SMOKE_CONTAINER_NAME="$project-indexer"
compose=(docker compose --env-file /dev/null --project-name "$project" --project-directory "$PWD" -f compose/postgres-runtime.yml -f "$scratch/fixture.yml")
cleanup() {
  result=$?
  trap - EXIT
  if (( result )); then "${compose[@]}" logs --no-color indexer >&2 || true; fi
  if ! "${compose[@]}" down --volumes; then
    if (( result == 0 )); then result=1; fi
  fi
  rm -rf "$scratch"
  exit "$result"
}
trap cleanup EXIT
"${compose[@]}" up -d --no-build --pull never --wait --wait-timeout 90 postgres anvil
"${compose[@]}" up -d --no-build --pull never indexer
export SMOKE_HEALTH_URL="http://$("${compose[@]}" port indexer 8080)/health"
python3 - <<'PY'
import json, os, time, urllib.error, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for _ in range(60):
    try:
        with opener.open(os.environ['SMOKE_HEALTH_URL'], timeout=2) as response:
            state = json.load(response)
        subscriptions = state['indexing']['subscriptions']
        assert len(subscriptions) == 1, state
        assert subscriptions[0]['status'] == 'live', state
        assert subscriptions[0]['last_synced_block'] == 0, state
        assert subscriptions[0]['lag_blocks'] == 0, state
        print('Minimal runtime indexed the local chain with a read-only project; one durable subscription is live at genesis.')
        break
    except (urllib.error.URLError, KeyError, AssertionError):
        time.sleep(1)
else:
    raise SystemExit('Minimal runtime did not reach healthy durable subscription state within 60 seconds')
PY
