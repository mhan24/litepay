#!/usr/bin/env bash
# Destructive only inside a dedicated disposable CI runner. Never run on a host
# with an existing LitePay container or data volume.
set -euo pipefail
if docker inspect litepay >/dev/null 2>&1 || docker volume inspect litepay-ci_litepay-data >/dev/null 2>&1; then
  echo 'Refusing to run recovery exercise with existing LitePay state' >&2
  exit 1
fi
root=$(mktemp -d)
cleanup() {
 docker rm -f litepay >/dev/null 2>&1 || true
 docker volume rm litepay-ci_litepay-data >/dev/null 2>&1 || true
 rm -rf "$root"
}
trap cleanup EXIT
export COMPOSE_PROJECT_NAME=litepay-ci
docker network inspect liteshop_default >/dev/null 2>&1 || docker network create liteshop_default >/dev/null
mkdir "$root/package"
cp deploy/Dockerfile deploy/docker-compose.yml .dockerignore "$root/package/"
cp deploy/config.yaml "$root/package/config.yaml"
cp /tmp/litepay-ci-binary "$root/package/litepay"
tar czf "$root/first.tar.gz" -C "$root/package" .
DEPLOY_PATH="$root/release" ARCHIVE="$root/first.tar.gz" bash scripts/deploy-activate.sh
curl -fsS http://127.0.0.1:3101/healthz >/dev/null
# A valid image that passes read-only database verification but never becomes healthy.
cat > "$root/package/litepay" <<'BAD'
#!/bin/sh
if [ "$1" = '-verify-db' ]; then exit 0; fi
sleep 300
BAD
chmod 755 "$root/package/litepay"
tar czf "$root/bad.tar.gz" -C "$root/package" .
if DEPLOY_PATH="$root/release" ARCHIVE="$root/bad.tar.gz" LP_HEALTH_ATTEMPTS=2 bash scripts/deploy-activate.sh; then
 echo 'Broken release unexpectedly succeeded' >&2
 exit 1
fi
curl -fsS http://127.0.0.1:3101/healthz >/dev/null
test "$(docker exec litepay id -u)" = 10001
docker exec litepay /app/litepay -verify-db /app/data/litepay.db
test -n "$(find "$root/release-backups" -name '*.tar.gz' -print -quit)"
echo 'Real Docker health failure restored the working release and SQLite snapshot.'
