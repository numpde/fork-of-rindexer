# Development entrypoints. Container preparation, copied inputs and lifecycle are
# owned here; Compose defines services and containers/development/run.sh runs
# commands inside them. The /native regression target uses installed build tools.
SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
# Each recipe needs one shell so setup variables and cleanup traps survive until
# its Docker command finishes. Make must also stop on an intermediate failure.
.ONESHELL:
.DEFAULT_GOAL := help
# Serialize this Make invocation; run.sh separately locks the shared build cache.
.NOTPARALLEL:
.PHONY: help dev/image/build deps/install deps/locks/update deps/check \
        tests/checks tests/posture rust/format/check rust/checks tests/postgres \
        tests/dynamic-bytes-array/native tests/dynamic-bytes-array/docker tests ci

# Rendering, execution and teardown always address the same file and project.
compose = docker compose --env-file containers/development/images.env \
          --project-name "$$compose_project" --project-directory "$$checkout_root" \
          -f compose/development.yml

# Establish one invocation's trusted image pins, temporary inputs and cleanup.
# This block does not acquire images or start services. Set the teardown flag
# before running Compose services; external build-cache volumes survive teardown.
define container_invocation
checkout_root=$$(pwd -P)
export RINDEXER_ROOT="$$checkout_root" COMPOSE_DISABLE_ENV_FILE=1
set -a
source containers/development/images.env
set +a
for image in "$$RUST_IMAGE" "$$NODE_IMAGE" "$$POSTGRES_IMAGE"; do
  if ! [[ "$$image" =~ @sha256:[a-f0-9]{64}$$ ]]; then
    echo 'Image inputs must use immutable digests.' >&2
    exit 2
  fi
done
scratch_dir=$$(mktemp -d)
compose_project="rindexer-dev-$$(id -u)-$$$$"
compose_cleanup_required=0
cleanup() {
  local exit_status=$$?
  trap - EXIT
  if (( compose_cleanup_required )); then
    # Log retrieval and Compose teardown must not replace a failed command's
    # status. A teardown failure still turns an otherwise successful run red.
    if (( exit_status != 0 )); then
      $(compose) logs --no-color postgres >&2 || true
    fi
    if ! $(compose) down --volumes --remove-orphans; then
      if (( exit_status == 0 )); then
        exit_status=1
      fi
    fi
  fi
  rm -rf "$$scratch_dir"
  exit "$$exit_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Callers supply checkout-relative paths. Reject bad inputs rather than skipping
# them, both when archiving source and when exporting replacement lockfiles.
require_regular_checkout_file() {
  local file="$$1" parent
  if ! test -f "$$file" || test -L "$$file"; then
    echo "Not a regular input: $$file" >&2
    exit 2
  fi
  parent=$$(dirname "$$file")
  while [[ "$$parent" != . ]]; do
    if test -L "$$parent"; then
      echo "Symlink input parent: $$parent" >&2
      exit 2
    fi
    parent=$$(dirname "$$parent")
  done
}
toolchain_fingerprint=$$( \
  sha256sum containers/development/Dockerfile containers/development/images.env rust-toolchain.toml | \
    sha256sum | cut -d' ' -f1)
toolchain_image="rindexer-dev-tools:$$toolchain_fingerprint"
endef

# Ordinary consumers never pull or build a missing toolchain implicitly.
define require_toolchain_image
if ! docker image inspect "$$toolchain_image" >/dev/null; then
  echo 'Run make dev/image/build first.' >&2
  exit 2
fi
endef

# Use the same stable, checked path list for fingerprinting and manifest-only
# acquisition. Preparation code contributes to the fingerprint too.
define dependency_inputs
git ls-files --deduplicate --cached --others --exclude-standard -- \
  Cargo.toml Cargo.lock '**/Cargo.toml' rust-toolchain.toml \
  graphql/package.json graphql/package-lock.json | LC_ALL=C sort > "$$scratch_dir/manifests"
while IFS= read -r file; do
  # These paths become line-delimited tar input, not arbitrary shell arguments.
  if ! [[ "$$file" =~ ^[A-Za-z0-9_.][A-Za-z0-9_./-]*$$ ]]; then
    echo "Unsupported manifest path: $$file" >&2
    exit 2
  fi
  require_regular_checkout_file "$$file"
done < "$$scratch_dir/manifests"
dependency_fingerprint=$$( \
  { printf '%s\n' "$$toolchain_fingerprint"; \
    while IFS= read -r file; do sha256sum -- "$$file"; done < "$$scratch_dir/manifests"; \
    sha256sum containers/development/dependencies.Dockerfile containers/development/prepare.py; \
  } | sha256sum | cut -d' ' -f1)
dependency_image="rindexer-dev-deps:$$dependency_fingerprint"
endef

# The networked builder receives metadata, never application source or a writable
# checkout. Both locked installation and explicit lock refresh use this context.
define dependency_build_context
mkdir -p "$$scratch_dir/input"
tar --verbatim-files-from -cf - -T "$$scratch_dir/manifests" | tar -xf - -C "$$scratch_dir/input"
cp containers/development/prepare.py "$$scratch_dir/prepare.py"
cp containers/development/dependencies.Dockerfile "$$scratch_dir/Dockerfile"
endef

# Prepare a source snapshot and persistent cache for the image supplied by the
# calling recipe. Image choice stays beside that recipe, not in a mode dispatcher.
# Local edits and unignored new files enter the archive; tracked deletions do not.
# This is not a secret scanner: sensitive tracked files still enter.
define offline_runner
$(container_invocation)
$(require_toolchain_image)
$(dependency_inputs)
export RINDEXER_RUNNER_IMAGE="$(1)"
if ! docker image inspect "$$RINDEXER_RUNNER_IMAGE" >/dev/null; then
  echo 'Dependencies are not prepared for these inputs; run make deps/install.' >&2
  exit 2
fi
git ls-files -z --deduplicate --cached --others --exclude-standard -- \
  Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .cargo core cli xtask \
  e2e-tests examples graphql tests/development > "$$scratch_dir/source-candidates"
: > "$$scratch_dir/source-files"
while IFS= read -r -d '' file; do
  if ! test -e "$$file" && ! test -L "$$file"; then
    continue
  fi
  require_regular_checkout_file "$$file"
  printf '%s\0' "$$file" >> "$$scratch_dir/source-files"
done < "$$scratch_dir/source-candidates"
# Do not let tar recurse into paths absent from the checked file list.
tar --null --no-recursion -T "$$scratch_dir/source-files" -cf "$$scratch_dir/source.tar"
chmod 0444 "$$scratch_dir/source.tar"
export RINDEXER_SOURCE_TAR="$$scratch_dir/source.tar"
checkout_fingerprint=$$(printf '%s\n%s' "$$checkout_root" "$$(id -u)" | sha256sum | cut -c1-16)
export RINDEXER_BUILD_VOLUME="rindexer-$$checkout_fingerprint-$${toolchain_fingerprint:0:16}"
docker volume create "$$RINDEXER_BUILD_VOLUME" >/dev/null
compose_cleanup_required=1
endef

# The ## annotations are the help source, kept beside the recipes they describe.
help: ## Show supported container-development commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_\/.-]+:.*## / {printf "%-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Preparation is explicit, never an implicit prerequisite of checks or tests.
dev/image/build: ## Prepare pinned toolchain and PostgreSQL images (network)
	@$(container_invocation)
	docker pull --platform linux/amd64 "$$RUST_IMAGE"
	docker pull --platform linux/amd64 "$$NODE_IMAGE"
	docker pull --platform linux/amd64 "$$POSTGRES_IMAGE"
	docker build --platform linux/amd64 --pull=false \
	  --build-arg RUST_IMAGE="$$RUST_IMAGE" --build-arg NODE_IMAGE="$$NODE_IMAGE" \
	  -t "$$toolchain_image" -f containers/development/Dockerfile containers/development

deps/install: ## Acquire locked dependencies from manifests only (network)
	@$(container_invocation)
	$(require_toolchain_image)
	$(dependency_inputs)
	$(dependency_build_context)
	docker build --platform linux/amd64 --pull=false \
	  --build-arg TOOLCHAIN_IMAGE="$$toolchain_image" --build-arg UPDATE_LOCKS=0 \
	  -t "$$dependency_image" "$$scratch_dir"

# This is the only target that overwrites checkout files. Export and check both
# locks offline before copying either one. The two final copies are not atomic:
# interruption can leave only one updated. Review the diff before deps/install.
deps/locks/update: ## Explicitly resolve and apply lock updates (network, scoped writes)
	@$(container_invocation)
	$(require_toolchain_image)
	$(dependency_inputs)
	$(dependency_build_context)
	lock_update_image="rindexer-dev-lock-update:$$dependency_fingerprint"
	docker build --platform linux/amd64 --pull=false \
	  --build-arg TOOLCHAIN_IMAGE="$$toolchain_image" --build-arg UPDATE_LOCKS=1 \
	  -t "$$lock_update_image" "$$scratch_dir"
	for file in Cargo.lock graphql/package-lock.json; do
	  require_regular_checkout_file "$$file"
	  docker run --rm --pull never --platform linux/amd64 --network none --read-only \
	    --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges \
	    --pids-limit 32 --memory 128m --cpus 1 --entrypoint cat \
	    "$$lock_update_image" "/opt/rindexer/inputs/$$file" > "$$scratch_dir/$$(basename "$$file")"
	  test -s "$$scratch_dir/$$(basename "$$file")"
	done
	cp "$$scratch_dir/Cargo.lock" Cargo.lock
	cp "$$scratch_dir/package-lock.json" graphql/package-lock.json
	echo 'Review the two lockfile changes, then run make deps/install.'

# Consumers check the rendered policy and require existing prepared images.
deps/check: tests/posture ## Verify prepared inputs and package the real GraphQL executable offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline verify

tests/checks: tests/posture ## Test the prepared GraphQL build boundary in the pinned toolchain
	@$(call offline_runner,$$toolchain_image)
	$(compose) run --rm --no-deps -T offline policy

# Run Compose on the host to substitute variables and write its configuration as JSON.
# A separate Python container checks that JSON for container privileges, mounts,
# resource limits and networks. The Compose services are not started.
# Required source-path and cache-volume-name variables get placeholder values;
# no source archive or build-cache volume is prepared.
# Passing verifies declared settings, not enforcement by running services.
tests/posture: ## Check rendered Compose mounts, permissions, resources, and networks
	@$(container_invocation)
	$(require_toolchain_image)
	export RINDEXER_RUNNER_IMAGE="$$toolchain_image" RINDEXER_SOURCE_TAR="$$scratch_dir/source.tar" RINDEXER_BUILD_VOLUME=rindexer-posture-unused
	: > "$$RINDEXER_SOURCE_TAR"
	$(compose) config --format json > "$$scratch_dir/compose.json"
	chmod 0444 "$$scratch_dir/compose.json"
	docker run --rm --pull never --platform linux/amd64 --network none --read-only \
	  --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges \
	  --pids-limit 64 --memory 256m --cpus 1 --tmpfs /tmp:rw,nosuid,nodev,noexec,size=64m \
	  --mount "type=bind,source=$$checkout_root/tests/repository,target=/tests,readonly" \
	  --mount "type=bind,source=$$scratch_dir/compose.json,target=/input/compose.json,readonly" \
	  --entrypoint python3 "$$toolchain_image" -B -m unittest discover -s /tests -v

rust/format/check: tests/posture ## Check Rust formatting without modifying the checkout
	@$(call offline_runner,$$toolchain_image)
	$(compose) run --rm --no-deps -T offline fmt

# Formatting spans the workspace; Clippy covers only rindexer and this integration
# target, not the upstream workspace/all-features linting command.
rust/checks: rust/format/check ## Run Clippy for the migrated PostgreSQL target offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline clippy

# Compile without a network before starting PostgreSQL. Test processes receive
# only the internal fixture URL, never Docker access from this host launcher.
tests/postgres: tests/posture ## Run PostgreSQL round trips and fixture isolation without Docker access
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests postgres

# Run only the loss-of-elements regression. A nonzero result is expected while
# the production converter remains unfixed; neither target needs PostgreSQL.
tests/dynamic-bytes-array/native: ## Run the dynamic bytes-array red test with native tools
	@cargo nextest run -p rindexer --lib \
	  -E 'test(/^database::dynamic_bytes_array_tests::abi_dynamic_bytes_array_preserves_elements$$/)' --no-capture

# Use prepared images, without image acquisition or database startup.
tests/dynamic-bytes-array/docker: tests/posture ## Run the dynamic bytes-array red test in the prepared offline container
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline dynamic-bytes-array

# These aggregates cover only the migrated slice, not the upstream workspace or
# ABI regression library tests. The focused red-test targets stay separate.
tests: tests/checks tests/posture tests/postgres ## Run the currently migrated container suites only

ci: rust/checks tests ## Validate the first container-development slice (not all upstream suites)
