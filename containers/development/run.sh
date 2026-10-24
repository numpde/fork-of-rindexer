#!/usr/bin/env bash
set -euo pipefail
mkdir -p /work /tmp/home /tmp/cargo
# One checkout shares a build volume. Never race GraphQL packaging or Cargo.
exec 9>/build/development.lock
flock 9
mkdir -p /build/tmp
export TMPDIR=/build/tmp
cd /work
tar -xf /input/source.tar
export HOME=/tmp/home CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/build/target
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=line-tables-only CARGO_NET_OFFLINE=true
export npm_config_offline=true npm_config_update_notifier=false npm_config_audit=false
case "${1:?missing command}" in
  policy) exec python3 -B -m unittest discover -s tests/development -v ;;
  fmt) exec cargo fmt --all -- --check ;;
esac
python3 /opt/rindexer/prepare.py verify
cp /opt/rindexer/cargo-config.toml "$CARGO_HOME/config.toml"
ln -s /opt/rindexer/graphql/node_modules graphql/node_modules
export PKG_CACHE_PATH=/opt/rindexer/pkg-cache
export RINDEXER_GRAPHQL_PREBUILT=/build/graphql/rindexer-graphql-linux-x64
mkdir -p /build/graphql
# Package the real server from prepared inputs. A failed package build must not
# replace a previously complete executable or its input fingerprint.
key=$(sha256sum graphql/index.js graphql/package.json graphql/package-lock.json /opt/rindexer/prepared.json | sha256sum | cut -d' ' -f1)
if ! test -f "$RINDEXER_GRAPHQL_PREBUILT" || ! test -f /build/graphql/input.sha256 || ! test "$(cat /build/graphql/input.sha256)" = "$key"; then
  (cd graphql && npm run build -- --targets node22-linux-x64 --output "${RINDEXER_GRAPHQL_PREBUILT}.new")
  mv "${RINDEXER_GRAPHQL_PREBUILT}.new" "$RINDEXER_GRAPHQL_PREBUILT"
  printf '%s\n' "$key" > /build/graphql/input.sha256
fi
case "$1" in
  compile) exec cargo nextest run --frozen -p rindexer --test postgres_bytea_array --no-run ;;
  unsigned-integers)
    # A cold dependency compilation is not part of the test execution timeout.
    cargo nextest run --frozen -p rindexer --lib --no-run \
      -E '(test(/unsigned_integer/) | test(/^manifest::contract::tests::test_pg_type_/)) & not test(/::postgres_/)'
    exec timeout --signal=TERM --kill-after=20s 900s cargo nextest run --frozen \
      -p rindexer --lib --no-fail-fast --no-capture \
      -E '(test(/unsigned_integer/) | test(/^manifest::contract::tests::test_pg_type_/)) & not test(/::postgres_/)'
    ;;
  unsigned-integers-compile)
    exec cargo nextest run --frozen -p rindexer --lib --no-run \
      -E 'test(/^database::(generate::)?unsigned_integer_tests::postgres_/)'
    ;;
  unsigned-integers-postgres)
    : "${RINDEXER_TEST_POSTGRES_URL:?missing explicit PostgreSQL fixture}"
    exec timeout --signal=TERM --kill-after=20s 900s cargo nextest run --frozen \
      -p rindexer --lib --test-threads 2 --no-fail-fast --no-capture \
      -E 'test(/^database::(generate::)?unsigned_integer_tests::postgres_/)'
    ;;
  dynamic-bytes-array)
    # Exercise the decoder/converter regression only; no database is required.
    exec timeout --signal=TERM --kill-after=20s 900s cargo nextest run --frozen \
      -p rindexer --lib \
      -E 'test(/^database::dynamic_bytes_array_tests::abi_dynamic_bytes_array_preserves_elements$/)' --no-capture
    ;;
  postgres)
    : "${RINDEXER_TEST_POSTGRES_URL:?missing explicit PostgreSQL fixture}"
    exec timeout --signal=TERM --kill-after=20s 900s cargo nextest run --frozen -p rindexer --test postgres_bytea_array --test-threads 2 --no-capture ;;
  clippy) exec cargo clippy --frozen --workspace --all-targets -- -D warnings ;;
  verify) printf '%s\n' 'Offline dependency inputs and GraphQL executable are ready.' ;;
  *) printf 'Unsupported development command: %s\n' "$1" >&2; exit 2 ;;
esac
