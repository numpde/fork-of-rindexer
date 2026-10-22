#!/usr/bin/env bash
# Host orchestration only; compilers, package managers, and tests run in containers.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$root"
export RINDEXER_ROOT="$root" COMPOSE_DISABLE_ENV_FILE=1
set -a
source containers/development/images.env
set +a
for image in "$RUST_IMAGE" "$NODE_IMAGE" "$POSTGRES_IMAGE"; do
  [[ "$image" =~ @sha256:[a-f0-9]{64}$ ]] || { echo 'Image inputs must use immutable digests.' >&2; exit 2; }
done
command=${1:?missing development command}
case "$command" in image|install|update-locks|posture|policy|fmt|verify|clippy|postgres) ;; *) echo 'Unsupported development command.' >&2; exit 2 ;; esac
temp=$(mktemp -d)
project="rindexer-dev-$(id -u)-$$"
active=0
compose() { docker compose --env-file containers/development/images.env --project-name "$project" --project-directory "$root" -f compose/development.yml "$@"; }
cleanup() {
  status=$?
  trap - EXIT
  if (( active )); then
    if (( status )); then compose logs --no-color postgres >&2 || true; fi
    compose down --volumes --remove-orphans || { if (( status == 0 )); then status=1; fi; }
  fi
  rm -rf "$temp"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

guard_file() {
  local file="$1" parent
  test -f "$file" && ! test -L "$file" || { echo "Not a regular input: $file" >&2; exit 2; }
  parent=$(dirname "$file")
  while [[ "$parent" != . ]]; do
    ! test -L "$parent" || { echo "Symlink input parent: $parent" >&2; exit 2; }
    parent=$(dirname "$parent")
  done
}
manifest_list() {
  git ls-files --deduplicate --cached --others --exclude-standard -- Cargo.toml Cargo.lock '**/Cargo.toml' rust-toolchain.toml graphql/package.json graphql/package-lock.json | LC_ALL=C sort
}
tool_key=$(sha256sum containers/development/Dockerfile containers/development/images.env rust-toolchain.toml | sha256sum | cut -d' ' -f1)
tool_image="rindexer-dev-tools:$tool_key"
if [[ "$command" == image ]]; then
  docker pull --platform linux/amd64 "$RUST_IMAGE"
  docker pull --platform linux/amd64 "$NODE_IMAGE"
  docker pull --platform linux/amd64 "$POSTGRES_IMAGE"
  docker build --platform linux/amd64 --pull=false --build-arg RUST_IMAGE="$RUST_IMAGE" --build-arg NODE_IMAGE="$NODE_IMAGE" -t "$tool_image" -f containers/development/Dockerfile containers/development
  exit
fi
docker image inspect "$tool_image" >/dev/null || { echo 'Run make dev/image/build first.' >&2; exit 2; }
if [[ "$command" == posture ]]; then
  export RINDEXER_RUNNER_IMAGE="$tool_image" RINDEXER_SOURCE_TAR="$temp/source.tar" RINDEXER_BUILD_VOLUME=rindexer-posture-unused
  : > "$RINDEXER_SOURCE_TAR"
  compose config --format json > "$temp/compose.json"
  chmod 0444 "$temp/compose.json"
  docker run --rm --pull never --platform linux/amd64 --network none --read-only \
    --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges \
    --pids-limit 64 --memory 256m --cpus 1 --tmpfs /tmp:rw,nosuid,nodev,noexec,size=64m \
    --mount "type=bind,source=$root/tests/repository,target=/tests,readonly" \
    --mount "type=bind,source=$temp/compose.json,target=/input/compose.json,readonly" \
    --entrypoint python3 "$tool_image" -B -m unittest discover -s /tests -v
  exit
fi
manifest_list > "$temp/manifests"
while IFS= read -r file; do
  [[ "$file" =~ ^[A-Za-z0-9_.][A-Za-z0-9_./-]*$ ]] || { echo "Unsupported manifest path: $file" >&2; exit 2; }
  guard_file "$file"
done < "$temp/manifests"
dep_key=$( { printf '%s\n' "$tool_key"; while IFS= read -r file; do sha256sum -- "$file"; done < "$temp/manifests"; sha256sum containers/development/dependencies.Dockerfile containers/development/prepare.py; } | sha256sum | cut -d' ' -f1)
dep_image="rindexer-dev-deps:$dep_key"
if [[ "$command" == install || "$command" == update-locks ]]; then
  mkdir -p "$temp/input"
  tar --verbatim-files-from -cf - -T "$temp/manifests" | tar -xf - -C "$temp/input"
  cp containers/development/prepare.py "$temp/prepare.py"
  cp containers/development/dependencies.Dockerfile "$temp/Dockerfile"
  update=0
  if [[ "$command" == update-locks ]]; then update=1; dep_image="rindexer-dev-lock-update:$dep_key"; fi
  docker build --platform linux/amd64 --pull=false --build-arg TOOLCHAIN_IMAGE="$tool_image" --build-arg UPDATE_LOCKS="$update" -t "$dep_image" "$temp"
  if (( update )); then
    # Networked acquisition never sees the checkout. Export only the two locks.
    for file in Cargo.lock graphql/package-lock.json; do
      guard_file "$file"
      docker run --rm --pull never --platform linux/amd64 --network none --read-only --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges --pids-limit 32 --memory 128m --cpus 1 --entrypoint cat "$dep_image" "/opt/rindexer/inputs/$file" > "$temp/$(basename "$file")"
      test -s "$temp/$(basename "$file")"
    done
    cp "$temp/Cargo.lock" Cargo.lock
    cp "$temp/package-lock.json" graphql/package-lock.json
    echo 'Review the two lockfile changes, then run make deps/install.'
  fi
  exit
fi
export RINDEXER_RUNNER_IMAGE="$dep_image"
if [[ "$command" == policy || "$command" == fmt ]]; then export RINDEXER_RUNNER_IMAGE="$tool_image"; fi
docker image inspect "$RINDEXER_RUNNER_IMAGE" >/dev/null || { echo 'Dependencies are not prepared for these inputs; run make deps/install.' >&2; exit 2; }
# Include local source edits but not .git, ignored files, host dependencies, or secrets.
git ls-files -z --deduplicate --cached --others --exclude-standard -- Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .cargo core cli xtask e2e-tests examples graphql tests/development > "$temp/source-candidates"
: > "$temp/source-files"
while IFS= read -r -d '' file; do
  if ! test -e "$file" && ! test -L "$file"; then continue; fi
  guard_file "$file"
  printf '%s\0' "$file" >> "$temp/source-files"
done < "$temp/source-candidates"
tar --null --no-recursion -T "$temp/source-files" -cf "$temp/source.tar"
chmod 0444 "$temp/source.tar"
export RINDEXER_SOURCE_TAR="$temp/source.tar"
repository_key=$(printf '%s\n%s' "$root" "$(id -u)" | sha256sum | cut -c1-16)
export RINDEXER_BUILD_VOLUME="rindexer-${repository_key}-${tool_key:0:16}"
docker volume create "$RINDEXER_BUILD_VOLUME" >/dev/null
active=1
if [[ "$command" == postgres ]]; then
  compose run --rm --no-deps -T offline compile
  compose up -d --no-build --pull never --wait --wait-timeout 90 postgres
  compose run --rm --no-deps -T postgres-tests postgres
else
  compose run --rm --no-deps -T offline "$command"
fi
