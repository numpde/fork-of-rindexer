#!/usr/bin/env bash
set -euo pipefail
image=${1:?supply the immutable local image ID printed by runtime/image/build}
if ! [[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]]; then
  echo 'Runtime smoke check requires an immutable image ID.' >&2; exit 2
fi
user=$(docker image inspect "$image" --format '{{.Config.User}}')
test "$user" = 65532:65532
entrypoint=$(docker image inspect "$image" --format '{{json .Config.Entrypoint}}')
test "$entrypoint" = '["/usr/local/bin/rindexer","start","--path","/project","indexer"]'
# Exercise the actual ELF loader and dependencies without network, credentials or
# project mounts. Container lifetime and filesystem writes end with this command.
docker run --rm --network none --read-only --user 65532:65532 --cap-drop ALL \
  --security-opt no-new-privileges --pids-limit 64 --memory 256m --cpus 1 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,uid=65532,gid=65532,mode=0700,size=16m \
  --entrypoint /usr/local/bin/rindexer "$image" --version
