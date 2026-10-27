# The context is generated offline: one CLI, its ELF dependencies, CA roots and
# this file. Compiler, Node, source, credentials and Foundry never enter it.
FROM scratch
COPY rootfs/ /
USER 65532:65532
WORKDIR /project
ENV HOME=/tmp SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/usr/local/bin/rindexer", "start", "--path", "/project", "indexer"]
