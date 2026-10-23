ARG TOOLCHAIN_IMAGE
FROM ${TOOLCHAIN_IMAGE}
USER 0:0
ARG UPDATE_LOCKS=0
ENV CARGO_HOME=/tmp/acquire PKG_CACHE_PATH=/opt/rindexer/pkg-cache
COPY input/ /input/
COPY prepare.py /opt/rindexer/prepare.py
RUN python3 /opt/rindexer/prepare.py acquire ${UPDATE_LOCKS} && \
    chown -R --no-dereference 65532:65532 /opt/rindexer && \
    rm -rf /tmp/acquire /root/.npm /input
USER 65532:65532
