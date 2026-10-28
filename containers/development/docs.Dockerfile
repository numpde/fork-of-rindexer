ARG NODE_IMAGE
FROM ${NODE_IMAGE}
# Vocs invokes git for page metadata even in a source tree without .git.
# Resolve that tool from a reviewed snapshot instead of today's package mirrors.
# The slim base lacks OS certificates; bootstrap APT with Node's bundled roots.
RUN node -e "const fs=require('node:fs'); fs.mkdirSync('/etc/ssl/certs',{recursive:true}); fs.writeFileSync('/etc/ssl/certs/ca-certificates.crt',require('node:tls').rootCertificates.join('\\n'))" && \
    rm -f /etc/apt/sources.list.d/* && \
    printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261020T000000Z bookworm main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261020T000000Z bookworm-updates main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20261020T000000Z bookworm-security main' \
      > /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false -o APT::Update::Error-Mode=any update && \
    apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/docs
# Networked preparation receives only locked dependency metadata, not source.
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund && chmod -R a+rX /opt/docs
COPY docs-run.sh /opt/docs/run.sh
USER 65532:65532
WORKDIR /work
ENTRYPOINT ["/bin/sh", "/opt/docs/run.sh"]
