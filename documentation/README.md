# Documentation

Maintain **Fork changes & migrations** (`docs/pages/docs/fork-changes/`), its
linked fork migration notes, and the fork changelog. Other site pages are a frozen
upstream snapshot; put new fork-specific guidance in the maintained section. Keep
the site-wide maintenance warning and its link visible.

The site uses [Vocs](https://vocs.dev). Run these commands from the repository
root with the selected local Docker engine and Compose. Node and npm run inside
containers; neither needs to be installed on the host.

```bash
make docs/deps/install  # explicit networked preparation from package-lock.json
make docs/build         # offline production build check
make docs/dev           # live edits: http://127.0.0.1:5173
make docs/preview       # build and serve the production site at the same address
```

Preparation uses the pinned Node image and receives only the docs package files
and runner. Repeat preparation when the package files, pinned Node image,
Dockerfile or runner change. Other commands require that prepared image and never
install dependencies implicitly.

Pages, public assets and Vocs/TypeScript configuration are mounted read-only.
`docs/dev` watches live edits. Dependencies, caches and build output stay in
container tmpfs; no `node_modules` or generated files are written to the checkout.
The executable work tmpfs allows esbuild's native helper. `docs/build` has no
network; dev and preview sessions use a named bridge network with outbound access
and publish only a loopback port. The browser-facing bridge is intentional: internal-only networks
do not publish ports on the supported Docker engine. No Docker socket or host
credentials enter a docs container. Pages/public directories are inputs,
including local files placed there: keep secrets out of those trees.

Use `DOCS_PORT=5174 make docs/dev` for another loopback port. Servers run in the
foreground: Ctrl-C stops the container and removes its network. The shared Make
limit defaults to 30 minutes and also applies to servers; extend it deliberately,
for example `make MAKE_TIMEOUT=2h docs/dev`.

`make docs/checks` checks the real rendered composition and entrypoints using the
prepared development tools image (`make dev/image/build`). Docs builds themselves
do not require the Rust dependency image, PostgreSQL, or GraphQL preparation.
