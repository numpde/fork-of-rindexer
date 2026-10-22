ARG NODE_IMAGE
ARG RUST_IMAGE
FROM ${NODE_IMAGE} AS node
FROM ${RUST_IMAGE}
# Package resolution is tied to a reviewed Debian snapshot, not today's mirrors.
RUN rm -f /etc/apt/sources.list.d/* && \
    printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261020T000000Z bookworm main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261020T000000Z bookworm-updates main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20261020T000000Z bookworm-security main' \
      > /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends ca-certificates clang libclang-dev \
      libssl-dev pkg-config cmake python3 git util-linux && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx
RUN rustup component add --toolchain 1.99.1 rustfmt clippy && \
    cargo install cargo-nextest --version 0.9.149 --locked --jobs 2 --root /opt/tools && \
    rm -rf /usr/local/cargo/registry /usr/local/cargo/git && \
    rustc --version && node --version && /opt/tools/bin/cargo-nextest --version
# Docker initializes an empty named volume from this directory's ownership.
# No root/chown helper, world-writable cache, or writable checkout is needed.
RUN install -d -m 0700 -o 65532 -g 65532 /build
ENV PATH="/opt/tools/bin:${PATH}" CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=line-tables-only RUSTUP_TOOLCHAIN=1.99.1
USER 65532:65532
WORKDIR /work
