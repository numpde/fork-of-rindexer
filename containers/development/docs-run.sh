#!/bin/sh
set -eu
export HOME=/tmp npm_config_offline=true npm_config_update_notifier=false
export npm_config_audit=false
cp /opt/docs/package.json /opt/docs/package-lock.json /work/
# Vite caches and native helpers stay in disposable container storage.
cp -R /opt/docs/node_modules /work/node_modules
case "${1:?missing docs command}" in
  build) exec npm run build ;;
  dev) exec npm run dev -- --host --port 5173 ;;
  preview)
    npm run build
    exec npm run preview
    ;;
  *) echo "Unsupported docs command: $1" >&2; exit 2 ;;
esac
