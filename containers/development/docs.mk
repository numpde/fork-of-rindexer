# Docs have separate locked dependencies and no Rust/GraphQL preparation.
DOCS_PORT ?= 5173
export DOCS_PORT
.PHONY: docs/deps/install docs/build docs/dev docs/preview docs/checks

define docs_invocation
$(container_invocation)
compose_file=compose/docs.yml
for file in documentation/package.json documentation/package-lock.json \
  containers/development/docs.Dockerfile containers/development/docs-run.sh \
  documentation/vocs.config.tsx documentation/tsconfig.json \
  documentation/docs/pages/index.mdx documentation/docs/public/favicon.png; do
  require_regular_checkout_file "$$file"
done
if ! [[ "$$DOCS_PORT" =~ ^[1-9][0-9]{0,4}$$ ]] || (( 10#$$DOCS_PORT > 65535 )); then
  echo 'DOCS_PORT must be a decimal port from 1 to 65535 without leading zeroes.' >&2; exit 2
fi
export RINDEXER_DOCS_PORT="$$DOCS_PORT"
docs_fingerprint=$$( { printf '%s\n' "$$NODE_IMAGE"; \
  sha256sum documentation/package.json documentation/package-lock.json \
    containers/development/docs.Dockerfile containers/development/docs-run.sh; \
  } | sha256sum | cut -d' ' -f1)
docs_image="rindexer-docs-deps:$$docs_fingerprint"
endef

define docs_consumer
$(docs_invocation)
if ! RINDEXER_DOCS_IMAGE=$$(docker image inspect --format '{{.Id}}' "$$docs_image"); then
  echo 'Cannot inspect the prepared docs image. Check the selected Docker engine; if the image is missing, run make docs/deps/install.' >&2
  exit 2
fi
export RINDEXER_DOCS_IMAGE
compose_cleanup_required=1
endef

docs/deps/install: ## Prepare locked docs dependencies in a pinned Node container (network)
	@$(docs_invocation)
	mkdir "$$scratch_dir/docs"
	cp documentation/package.json documentation/package-lock.json "$$scratch_dir/docs/"
	cp containers/development/docs.Dockerfile "$$scratch_dir/docs/Dockerfile"
	cp containers/development/docs-run.sh "$$scratch_dir/docs/"
	docker pull --platform linux/amd64 "$$NODE_IMAGE"
	docker build --platform linux/amd64 --pull=false --build-arg NODE_IMAGE="$$NODE_IMAGE" \
	  -t "$$docs_image" "$$scratch_dir/docs"

docs/build: ## Validate the docs production build offline without host Node/npm or output writes
	@$(docs_consumer)
	$(compose) run --rm --no-deps -T build

docs/dev: ## Serve live docs edits on loopback only (default DOCS_PORT=5173)
	@$(docs_consumer)
	$(compose) run --rm --no-deps --service-ports -T dev

docs/preview: ## Build and serve production docs on loopback only (default DOCS_PORT=5173)
	@$(docs_consumer)
	$(compose) run --rm --no-deps --service-ports -T preview

docs/checks: ## Check docs container entrypoints and rendered authority without starting Vocs
	@$(docs_invocation)
	$(require_toolchain_image)
	export RINDEXER_DOCS_IMAGE="$$docs_image"
	$(compose) config --format json > "$$scratch_dir/docs-compose.json"
	chmod 0444 "$$scratch_dir/docs-compose.json"
	docker run --rm --pull never --network none --read-only --user 65532:65532 \
	  --cap-drop ALL --security-opt no-new-privileges:true --memory 256m --cpus 1 --pids-limit 64 \
	  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=32m \
	  --mount "type=bind,source=$$checkout_root/Makefile,target=/repo/Makefile,readonly" \
	  --mount "type=bind,source=$$checkout_root/tools,target=/repo/tools,readonly" \
	  --mount "type=bind,source=$$checkout_root/containers/development,target=/repo/containers/development,readonly" \
	  --mount "type=bind,source=$$checkout_root/tests/docs,target=/repo/tests/docs,readonly" \
	  --mount "type=bind,source=$$scratch_dir/docs-compose.json,target=/input/docs-compose.json,readonly" \
	  --workdir /repo --env DOCS_COMPOSE_JSON=/input/docs-compose.json \
	  --entrypoint python3 "$$toolchain_image" -B -m unittest discover -s tests/docs -v
