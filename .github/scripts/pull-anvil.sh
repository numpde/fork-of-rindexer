#!/usr/bin/env bash
set -euo pipefail

image=$(<core/src/indexer/fixtures/anvil-image.txt)
for attempt in 1 2 3; do
  if timeout --kill-after=10s 120s docker pull "$image"; then
    exit 0
  fi
  if (( attempt < 3 )); then
    echo "Anvil image pull failed (attempt $attempt of 3); retrying in 5 seconds." >&2
    sleep 5
  fi
done
echo "Cannot pull Anvil fixture image $image after 3 attempts." >&2
exit 1
