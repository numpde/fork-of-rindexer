# Selected contracts to PostgreSQL on one host

This optional Linux/amd64 runtime uses the maintained, pinned development inputs
and locked dependencies to compile only `rindexer_cli --no-default-features`.
The offline package contains the CLI, its resolved ELF libraries and CA roots.
The final scratch image runs as uid/gid 65532; it includes no shell, compiler,
Node installation, Foundry, Kafka or embedded reth. The CLI still embeds the
GraphQL payload at build time; its fixed `start indexer` entrypoint never starts it.
The root Dockerfile and published upstream image are unchanged.

Build explicitly from the repository root after preparing the development inputs:

```bash
make dev/image/build
make deps/install
make runtime/image/build
```

The first two commands acquire dependencies; ordinary image packaging is offline.
Run `make runtime/checks` to validate the rendered deployment and its rejection
cases without starting services. The final build command prints an immutable local image ID.
Run `bash containers/runtime/smoke.sh sha256:...` to execute that actual image with
no network or credentials and a read-only filesystem. For the full startup check,
run `bash containers/runtime/startup-smoke.sh sha256:...` with the pinned PostgreSQL
and Anvil images present. It boots this profile against local fixtures, verifies
one durable subscription through HTTP health, and removes its unique fixture
containers and volume; it never reuses the deployment database. The startup
fixture has the profile's routable RPC bridge, uses only local fixture endpoints,
and publishes health on a temporary loopback port. It packages the current
working tree, including local edits; commit and review that tree for deployment.
It sends only the generated runtime context to Docker, never a project directory,
`.env`, source tree or credentials. The final image build has no network access.
The same `images.env` pins the PostgreSQL image. Acquire that image explicitly if
it is not already present; service startup uses `--pull never` and never builds.

For backfill, downtime catch-up, reorg recovery and restart against the actual
runtime image, use the self-contained lifecycle check:

```bash
make MAKE_TIMEOUT=8m runtime/lifecycle/checks RINDEXER_IMAGE=sha256:...
```

It requires a local rootless Docker context (`rootless` by default; override with
`RINDEXER_DOCKER_CONTEXT`) and the pinned PostgreSQL/Anvil images already present.
It never builds or pulls, reads no sibling checkout, and uses the minimal `Funded`
ABI under `fixtures/lifecycle/`, not a payment contract implementation. The host
runs the Python standard-library orchestrator; the three non-root services have a
combined one-CPU limit and no Docker socket. The private database volume and
unique Compose project are removed on success, failure or handled cancellation.
Health uses a temporary loopback port; the runtime profile's RPC bridge remains
routable. Each phase prints JSON evidence; the final success record includes the
input image ID and optional revision label. Historical images are accepted for
red/green reproduction. A teardown failure fails the check and retains private
fixture inputs with a recovery command. `MAKE_TIMEOUT` covers the whole recipe
with the existing cleanup grace period. See the maintained
[fork guide](../../documentation/docs/pages/docs/fork-changes/index.mdx#runtime-lifecycle-integration-check)
for assertions and scope.

Copy `containers/runtime/project/` to a dedicated operator-owned directory. Copy
`environment.example` outside the checkout to an absolute file, set mode 0600,
and replace every placeholder. Use the immutable local image ID, a random
hexadecimal PostgreSQL password, your proxy URL, chain ID, selected contract and
inclusive first block. Keep the project directory free of `.env` files. Review
schema changes before starting; the entrypoint does not auto-confirm migrations.
The supplied ABI and subscription select only ERC-20 `Transfer`; change both for
your actual selected events. Reorg handling is explicitly enabled and shared tip
logs disabled, so this small-subscription profile uses filtered log requests.
The indexer can reach the internet on its RPC network; this profile does not
restrict egress destinations or RPC methods. Enforce those limits at your proxy.

```bash
bash containers/runtime/deploy.sh start /absolute/path/runtime.env
curl --fail http://127.0.0.1:8080/health
bash containers/runtime/deploy.sh status /absolute/path/runtime.env
bash containers/runtime/deploy.sh stop /absolute/path/runtime.env
```

Use Docker Engine with Compose v2, Python 3, Bash and `flock`, with a local
Unix-socket context. Set `RINDEXER_DOCKER_CONTEXT=rootless` in the private runtime
env file to use an existing rootless engine. Files omitting this
optional key retain `default`; the launcher never changes your global context.
It resolves the named context once and pins its socket for all commands in that
invocation. Missing contexts, remote endpoints and unavailable engines fail
without falling back to another engine. Changing this key selects a different
engine and its volumes; it does not migrate the database.
The launcher rejects ambient engine/Compose overrides, validates the rendered
configuration without printing credentials, and serializes operations with a host
lock. `start` first stops and waits for the old indexer, then starts PostgreSQL
and its replacement. The fixed container name prevents scaling this service or
starting a second copy under another Compose project on the same engine.
Automatic restarts are disabled so a terminal indexing failure remains visible.
Monitor HTTP status, `failed_streams`, and each subscription's `lag_blocks` with
an application-specific freshness threshold; HTTP 200 during catch-up is liveness.
A missing or growing cursor must not be treated as completion.

**This is a single-host deployment, not a distributed ownership lock.** Do not
start custom CLI/Rust writers, another host, or an overlapping rolling deployment
against this database. Receipts and process-local reorg barriers do not fence such
writers. A session advisory lock by itself would also not fence pooled writes
after its owning connection is lost. Distributed failover requires a separately
designed database fencing protocol and is outside this profile.

PostgreSQL joins only an internal network and publishes no host port. Its password
is passed only to PostgreSQL and the indexer; the Docker operator can inspect these
environment values. Only health is published, bound to host loopback. Both services
have read-only root filesystems, dropped capabilities and bounded resources/logs.
The project bind is read-only; scratch files live in tmpfs. PostgreSQL's named
volume persists across `stop` and upgrades. Back it up before upgrading. No reset
command is provided: deleting that volume destroys raw data, derived state,
receipts and progress. Reconcile or rebuild ranges corrupted by older versions;
these fixes do not automatically repair previously committed history.
