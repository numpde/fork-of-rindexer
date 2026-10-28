# Development entrypoints. Container preparation, copied inputs and lifecycle are
# owned here; Compose defines services and containers/development/run.sh runs
# commands inside them. The /native regression target uses installed build tools.
include tools/make-timeout.mk
.DEFAULT_GOAL := help
# Serialize this Make invocation; run.sh separately locks the shared build cache.
.NOTPARALLEL:
.PHONY: help dev/image/build deps/install deps/locks/update deps/check \
        tests/checks tests/posture rust/format/check rust/checks tests/postgres \
        tests/dynamic-bytes-array/native tests/dynamic-bytes-array/docker tests ci

# Rendering, execution and teardown always address the same file and project.
compose = docker compose --env-file containers/development/images.env \
          --project-name "$$compose_project" --project-directory "$$checkout_root" \
          -f compose/development.yml "$${compose_profile_args[@]}"

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
compose_profile_args=()
unset COMPOSE_PROFILES
compose_cleanup_required=0
cleanup() {
  local exit_status=$$?
  trap - EXIT
  # Finish bounded cleanup even if cancellation is delivered more than once.
  trap '' TERM INT
  if (( compose_cleanup_required )); then
    # Log retrieval and Compose teardown must not replace a failed command's
    # status. A teardown failure still turns an otherwise successful run red.
    if (( exit_status != 0 )); then
      timeout --signal=TERM --kill-after=5s 5s $(compose) logs --no-color postgres >&2 || true
    fi
    if ! timeout --signal=TERM --kill-after=5s 35s $(compose) down --volumes --remove-orphans; then
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
# Native Testcontainers and Compose share the test fixture's immutable image.
require_regular_checkout_file core/src/indexer/fixtures/anvil-image.txt
export ANVIL_IMAGE=$$(cat core/src/indexer/fixtures/anvil-image.txt)
if ! [[ "$$ANVIL_IMAGE" =~ ^ghcr.io/foundry-rs/foundry@sha256:[a-f0-9]{64}$$ ]]; then
  echo 'The Anvil fixture image must use its reviewed repository and immutable digest.' >&2
  exit 2
fi
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
  e2e-tests examples graphql tests/development containers/development/typings.py \
  containers/runtime/Dockerfile containers/runtime/package.py > "$$scratch_dir/source-candidates"
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
define refresh_typings
# Source generation and host writes use separate offline containers. The writer
# sees only Cargo metadata, the archive, and the existing generated directories.
selected_context=$$(docker context show)
if [[ "$$(docker context inspect "$$selected_context" --format '{{.Endpoints.docker.Host}}')" != unix:///* ]]; then
  echo 'Typing refresh requires a local Unix-socket Docker context for output binds.' >&2; exit 2
fi
unset DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH DOCKER_API_VERSION
export DOCKER_CONTEXT="$$selected_context"
if (( $$(id -u) == 0 )); then
  echo 'Run typing refresh as the non-root checkout owner.' >&2; exit 2
fi
$(call offline_runner,$$dependency_image)
case "$$checkout_root" in *','*|*$$'\n'*) echo 'Checkout path cannot be encoded as a Docker bind mount.' >&2; exit 2;; esac
$(compose) run --rm --no-deps -T offline codegen-typings > "$$scratch_dir/typings.tar"
chmod 0444 "$$scratch_dir/typings.tar"
typing_inputs=(--mount "type=bind,src=$$checkout_root/Cargo.toml,dst=/repo/Cargo.toml,readonly" \
  --mount "type=bind,src=$$checkout_root/containers/development/typings.py,dst=/tools/typings.py,readonly")
typing_runtime=(--rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 64 --memory 256m --cpus 1 --tmpfs /tmp:rw,nosuid,nodev,noexec,size=64m)
# Rootless container uid 0 maps to the non-root daemon owner on the host.
# A rootful daemon instead needs the caller's explicit uid and gid.
typing_user="$$(id -u):$$(id -g)"
docker_security=$$(docker info --format '{{range .SecurityOptions}}{{println .}}{{end}}')
while IFS= read -r option; do
  if [[ "$$option" == name=rootless ]]; then typing_user=0:0; fi
done <<< "$$docker_security"
docker run "$${typing_runtime[@]}" --user 65532:65532 "$${typing_inputs[@]}" \
  --entrypoint python3 "$$toolchain_image" -B /tools/typings.py roots --root /repo > "$$scratch_dir/typing-roots"
readonly_output=
if [[ '$(1)' == check ]]; then readonly_output=,readonly; fi
while IFS= read -r output; do
  if ! test -d "$$output" || test -L "$$output"; then
    echo "Typing output directory must already exist without symlinks: $$output" >&2; exit 2
  fi
  parent="$$output"
  while [[ "$$parent" != . ]]; do
    if test -L "$$parent"; then echo "Symlink output parent: $$parent" >&2; exit 2; fi
    parent=$$(dirname "$$parent")
  done
  typing_inputs+=(--mount "type=bind,src=$$checkout_root/$$output,dst=/repo/$$output$$readonly_output")
done < "$$scratch_dir/typing-roots"
docker run "$${typing_runtime[@]}" --user "$$typing_user" "$${typing_inputs[@]}" \
  --mount "type=bind,src=$$scratch_dir/typings.tar,dst=/input/typings.tar,readonly" \
  --entrypoint python3 "$$toolchain_image" -B /tools/typings.py '$(1)' --root /repo --archive /input/typings.tar
if [[ '$(1)' == update ]]; then
  while IFS= read -r output; do
    while IFS= read -r -d '' file; do
      if ! test -e "$$file" && ! test -L "$$file"; then continue; fi
      if [[ "$$(stat -c '%u' "$$file")" != "$$(id -u)" ]]; then
        echo "Generated file is not owned by the checkout user: $$file" >&2; exit 2
      fi
    done < <(git ls-files -z --cached --others --exclude-standard --deduplicate -- "$$output")
  done < "$$scratch_dir/typing-roots"
fi
endef

.PHONY: codegen/typings/check codegen/typings/update
codegen/typings/check: tests/posture ## Generate workspace example typings offline and report drift without changing source
	@$(call refresh_typings,check)

codegen/typings/update: tests/posture ## Regenerate only workspace example typings (writes generated Rust source)
	@$(call refresh_typings,update)

.PHONY: tests/trace-bindings/docker
tests/trace-bindings/docker: tests/posture ## Generate and compile trace bindings in a copied example without changing its configuration
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline trace-bindings

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

# Lock refresh overwrites the two dependency locks. Export and check both
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

tests/checks: tests/posture ## Test prepared GraphQL inputs and generated typing archive admission
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
	# Include the optional chain fixture in policy checks; config starts no services.
	compose_profile_args=(--profile native-transfers)
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

# Match upstream's workspace lint command, including the regression test code.
rust/checks: rust/format/check ## Run workspace Clippy for all targets offline
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

.PHONY: tests/postgres-tls/docker

.PHONY: tests/event-inputs/docker tests/event-inputs/postgres

.PHONY: tests/checkpoint-order/docker

.PHONY: tests/permanent-failures/docker tests/permanent-failures/postgres

.PHONY: tests/postgres-replay/docker
.PHONY: tests/reorg/docker
tests/reorg/docker: tests/posture ## Check shared network rewinds, reorg validation and stale-work fencing offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline reorg

tests/postgres-replay/docker: tests/posture ## Check uncertain PostgreSQL commits, replayed events and derived effects
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline postgres-replay-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests postgres-replay

tests/permanent-failures/docker: tests/posture ## Check terminal callback handling, transient retries and stream health offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline permanent-failures

tests/permanent-failures/postgres: tests/posture ## Check PostgreSQL text NUL rejection without rejecting binary values
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline permanent-failures-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests permanent-failures-postgres

tests/checkpoint-order/docker: tests/posture ## Check concurrent callbacks and durable checkpoint ordering offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline checkpoint-order

tests/event-inputs/docker: tests/posture ## Check event inputs and nearby fetch/callback regressions offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline event-inputs

tests/event-inputs/postgres: tests/posture ## Check decode rejection, unchanged PostgreSQL rows/cursor and corrected replay
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline event-inputs-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests event-inputs-postgres

# Inject TLS negotiation failures in a loopback proxy to the disposable fixture.
tests/postgres-tls/docker: tests/posture ## Check PostgreSQL TLS policy and reconnect behavior
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline postgres-tls-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests postgres-tls

# Unsigned integer regressions: no dependency acquisition and no Docker access
# inside tests. Keep pure encoding checks separate from real PostgreSQL readback.
.PHONY: tests/unsigned-integers/docker tests/unsigned-integers/postgres

tests/unsigned-integers/docker: tests/posture ## Check unsigned event/custom-table types, binary encoding and signed controls offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline unsigned-integers

tests/unsigned-integers/postgres: tests/posture ## Check unsigned writes, schema checks and blockchain reorg reversal in PostgreSQL
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline unsigned-integers-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests unsigned-integers-postgres

.PHONY: tests/signed-integers/docker tests/signed-integers/postgres
tests/signed-integers/docker: tests/posture ## Check ABI signed integer bind types, binary encoding and compatibility controls offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline signed-integers

tests/signed-integers/postgres: tests/posture ## Read back signed integer scalars and arrays through PostgreSQL INSERT and COPY
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline signed-integers-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests signed-integers-postgres

.PHONY: tests/nested-arrays/docker tests/nested-arrays/postgres

tests/nested-arrays/docker: tests/posture ## Check nested tuple-array JSON after ABI decoding offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline nested-arrays

tests/nested-arrays/postgres: tests/posture ## Read back nested tuple-array JSON through PostgreSQL INSERT and COPY
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline nested-arrays-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests nested-arrays-postgres

.PHONY: tests/indexed-topic-hashes/docker tests/indexed-topic-hashes/postgres

tests/indexed-topic-hashes/docker: tests/posture ## Check indexed array/tuple topic hashes offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline indexed-topic-hashes

tests/indexed-topic-hashes/postgres: tests/posture ## Read back indexed array/tuple hashes through PostgreSQL INSERT and COPY
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline indexed-topic-hashes-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests indexed-topic-hashes-postgres

.PHONY: tests/fixed-bytes/docker tests/fixed-bytes/postgres

tests/fixed-bytes/docker: tests/posture ## Check fixed-byte logical lengths and trailing zeros offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline fixed-bytes

tests/fixed-bytes/postgres: tests/posture ## Read back fixed-byte values through PostgreSQL INSERT and COPY
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline fixed-bytes-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests fixed-bytes-postgres

.PHONY: tests/nested-tuples/docker

tests/nested-tuples/docker: tests/posture ## Check nested tuple JSON and the PostgreSQL/webhook callback together
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline nested-tuples-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests nested-tuples

.PHONY: tests/native-transfer-status/image tests/native-transfer-status/docker

tests/native-transfer-status/image: ## Acquire the pinned Anvil fixture image (network)
	@$(container_invocation)
	docker pull --platform linux/amd64 "$$ANVIL_IMAGE"

tests/native-transfer-status/docker: tests/posture ## Check native credits against accepting/reverting contracts on a disposable chain
	@$(call offline_runner,$$dependency_image)
	if ! docker image inspect "$$ANVIL_IMAGE" >/dev/null; then
	  echo 'Run make tests/native-transfer-status/image first.' >&2; exit 2
	fi
	$(compose) run --rm --no-deps -T offline native-transfer-status-compile
	compose_profile_args=(--profile native-transfers)
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres anvil
	$(compose) run --rm --no-deps -T -e RINDEXER_TEST_ANVIL_URL=http://anvil:8545 postgres-tests native-transfer-status

.PHONY: tests/view-call-reorg/docker
tests/view-call-reorg/docker: tests/posture ## Check historical calls, late RPC replies and PostgreSQL replacement state after rollback
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline view-call-reorg-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests view-call-reorg

.PHONY: tests/native-transfer-recovery/docker
tests/native-transfer-recovery/docker: tests/posture ## Check queued native work, replacement-prefix checkpoints and restart recovery
	@$(call offline_runner,$$dependency_image)
	if ! docker image inspect "$$ANVIL_IMAGE" >/dev/null; then
	  echo 'Run make tests/native-transfer-status/image first.' >&2; exit 2
	fi
	$(compose) run --rm --no-deps -T offline native-transfer-recovery-compile
	compose_profile_args=(--profile native-transfers)
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres anvil
	$(compose) run --rm --no-deps -T -e RINDEXER_TEST_ANVIL_URL=http://anvil:8545 postgres-tests native-transfer-recovery

.PHONY: tests/view-call-decoding/docker
tests/view-call-decoding/docker: tests/posture ## Check large view-call tuples and explicit return types through RPC and caches
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline view-call-decoding

.PHONY: tests/array-shapes/docker tests/array-shapes/postgres

tests/array-shapes/docker: tests/posture ## Check fixed and direct nested ABI array schemas and values offline
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline array-shapes

tests/array-shapes/postgres: tests/posture ## Read back fixed and direct nested ABI arrays through PostgreSQL INSERT and COPY
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline array-shapes-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests array-shapes-postgres

.PHONY: codegen/arrays/check
codegen/arrays/check: tests/posture ## Compile generated Rust handlers for fixed and nested ABI arrays in a disposable example
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline array-bindings

.PHONY: tests/callback-recovery/docker
tests/callback-recovery/docker: tests/posture ## Verify admitted callback retries retire before reorg rollback waits for writers
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline callback-recovery

.PHONY: tests/sequence-ordering/postgres
tests/sequence-ordering/postgres: tests/posture ## Verify custom-table ordering across high transaction indices and block boundaries
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline sequence-ordering-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests sequence-ordering

.PHONY: tests/native-startup/docker
tests/native-startup/docker: tests/posture ## Verify the first required native block through production startup on PostgreSQL and Anvil
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline native-startup-compile
	compose_profile_args=(--profile native-transfers)
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres anvil
	$(compose) run --rm --no-deps -T -e RINDEXER_TEST_ANVIL_URL=http://anvil:8545 postgres-tests native-startup

.PHONY: runtime/image/build
runtime/image/build: tests/posture ## Build the minimal PostgreSQL CLI image offline (no Kafka, reth or Foundry)
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline runtime-image > "$$scratch_dir/runtime.tar"
	docker build --network none --tag rindexer-postgres:local - < "$$scratch_dir/runtime.tar"
	docker image inspect rindexer-postgres:local --format '{{.Id}}'

.PHONY: runtime/checks
runtime/checks: runtime/launcher/checks ## Check the rendered single-host PostgreSQL deployment without starting services
	@python3 -B containers/runtime/test.py
	bash -n containers/runtime/deploy.sh containers/runtime/smoke.sh containers/runtime/startup-smoke.sh


.PHONY: tests/make-timeouts
tests/make-timeouts: ## Check recipe deadlines, child cancellation and cleanup without Docker
	@python3 -B -m unittest discover -s tests/make -p test_make_timeouts.py -v

.PHONY: runtime/launcher/checks
runtime/launcher/checks: ## Check actual launcher engine selection with offline Compose rendering
	@python3 -B -m unittest discover -s tests/runtime -v

.PHONY: tests/resume-without-start-block/docker
tests/resume-without-start-block/docker: ## Check saved subscription cursors when start_block is omitted
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline resume-without-start-block

.PHONY: tests/advanced-table-manifest/docker
tests/advanced-table-manifest/docker: ## Validate advanced table manifests using owned temporary fixtures
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline advanced-table-manifest

.PHONY: tests/resume-without-start-block/postgres
tests/resume-without-start-block/postgres: ## Verify downtime recovery through production PostgreSQL startup and callbacks
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline postgres-resume-without-start-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests postgres-resume-without-start

.PHONY: tests/historical-identity/postgres
tests/historical-identity/postgres: ## Reject stale-fork historical logs through sequential and parallel PostgreSQL startup
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline historical-identity-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests historical-identity

.PHONY: tests/journal-keys/postgres
tests/journal-keys/postgres: ## Verify composite journal keys and schema migration admission through PostgreSQL recovery
	@$(call offline_runner,$$dependency_image)
	$(compose) run --rm --no-deps -T offline journal-keys-compile
	$(compose) up -d --no-build --pull never --wait --wait-timeout 90 postgres
	$(compose) run --rm --no-deps -T postgres-tests journal-keys
