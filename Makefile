.DEFAULT_GOAL := help
.NOTPARALLEL:
.PHONY: help dev/image/build deps/install deps/locks/update deps/check tests/checks tests/posture rust/format/check rust/checks tests/postgres tests ci

help: ## Show supported container-development commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_\/.-]+:.*## / {printf "%-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev/image/build: ## Prepare pinned toolchain and PostgreSQL images (network)
	@bash tools/development.sh image

deps/install: ## Acquire locked dependencies from manifests only (network)
	@bash tools/development.sh install

deps/locks/update: ## Explicitly resolve and apply lock updates (network, scoped writes)
	@bash tools/development.sh update-locks

deps/check: tests/posture ## Verify prepared inputs and package the real GraphQL executable offline
	@bash tools/development.sh verify

tests/checks: tests/posture ## Test the prepared GraphQL build boundary in the pinned toolchain
	@bash tools/development.sh policy

tests/posture: ## Check rendered Compose mounts, permissions, resources, and networks
	@bash tools/development.sh posture

rust/format/check: tests/posture ## Check Rust formatting without modifying the checkout
	@bash tools/development.sh fmt

rust/checks: rust/format/check ## Run Clippy for the migrated PostgreSQL target offline
	@bash tools/development.sh clippy

tests/postgres: tests/posture ## Run PostgreSQL round trips and fixture isolation without Docker access
	@bash tools/development.sh postgres

tests: tests/checks tests/posture tests/postgres ## Run the currently migrated container suites only

ci: rust/checks tests ## Validate the first container-development slice (not all upstream suites)
