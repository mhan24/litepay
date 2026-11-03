#!/usr/bin/env bash
# Runs on the deployment host. No live volume is copied; workers/HTTP stay gated
# until validation succeeds. Failure before release restores the stopped snapshot.
set -euo pipefail
umask 077
: "${DEPLOY_PATH:?}" "${ARCHIVE:?}"
stamp=$(date -u +%Y%m%dT%H%M%SZ)-$$
backup_dir="${DEPLOY_PATH}-backups/$stamp"
stage_dir=$(mktemp -d)
old_name="litepay-rollback-$stamp"
old_image=""
old_stopped=0
old_renamed=0
released=0
volumes=()
saved_volumes=()

rollback() {
  code=$?
  trap - EXIT
  if (( code != 0 && old_stopped == 1 && released == 0 )); then
    echo 'Deployment failed; restoring previous version and offline data snapshot.' >&2
    if (( old_renamed == 1 )); then
      # New workers never ran while gated, so this snapshot cannot discard a
      # newly acknowledged payment or callback.
      docker stop litepay >/dev/null 2>&1 || true
      docker rm litepay >/dev/null 2>&1 || true
      restore_ok=1
      for volume in "${saved_volumes[@]}"; do
        docker run --rm --user 0:0 --entrypoint /bin/sh \
          -v "$volume:/data" -v "$backup_dir:/backup:ro" "$old_image" \
          -c 'find /data -mindepth 1 -maxdepth 1 -exec rm -rf {} + && tar xzf "/backup/$1.tar.gz" -C /data' sh "$volume" || restore_ok=0
      done
      if [[ -f "$backup_dir/release.tar.gz" ]]; then
        tar xzf "$backup_dir/release.tar.gz" -C "$DEPLOY_PATH" || restore_ok=0
      fi
      if (( restore_ok == 1 )); then
        docker tag "$old_image" litepay:go
        docker rename "$old_name" litepay
        docker start litepay >/dev/null
        echo "Previous version restored; backup retained: $backup_dir" >&2
      else
        echo "RESTORE FAILED: service remains stopped; preserve $backup_dir and reconcile manually." >&2
      fi
    else
      docker start litepay >/dev/null || true
    fi
  fi
  rm -rf "$stage_dir"
  rm -f "$ARCHIVE"
  exit "$code"
}
trap rollback EXIT

tar tzf "$ARCHIVE" >/dev/null
tar xzf "$ARCHIVE" -C "$stage_dir"
(cd "$stage_dir" && docker compose config >/dev/null)
# Build a unique tag; never replace the last working image before validation.
new_image="litepay:release-$stamp"
docker build -t "$new_image" "$stage_dir" >/dev/null

mkdir -p "$DEPLOY_PATH" "$backup_dir"
chmod 700 "${DEPLOY_PATH}-backups" "$backup_dir"
if docker inspect litepay >/dev/null 2>&1; then
  old_image=$(docker inspect -f '{{.Image}}' litepay)
  while IFS= read -r volume; do
    [[ -z "$volume" ]] || volumes+=("$volume")
  done < <(docker inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/app/data")}}{{.Name}}{{"\n"}}{{end}}{{end}}' litepay)
  [[ ${#volumes[@]} == 1 ]] || { echo 'Expected one SQLite data volume' >&2; exit 1; }
  # Require a healthy baseline. Do not call an already broken release a recovery point.
  curl -fsS -m 3 http://127.0.0.1:3101/healthz >/dev/null
  docker stop litepay >/dev/null
  old_stopped=1
  docker rename litepay "$old_name"
  old_renamed=1
  tar czf "$backup_dir/release.tar.gz" -C "$DEPLOY_PATH" .
  for volume in "${volumes[@]}"; do
    docker run --rm --user 0:0 --entrypoint /bin/sh \
      -v "$volume:/data:ro" -v "$backup_dir:/backup" "$old_image" \
      -c 'tar czf "/backup/$1.tar.gz" -C /data .' sh "$volume"
    tar tzf "$backup_dir/$volume.tar.gz" >/dev/null
    saved_volumes+=("$volume")
  done
fi

for name in litepay config.yaml Dockerfile docker-compose.yml .dockerignore; do
  install -m 640 "$stage_dir/$name" "$DEPLOY_PATH/$name"
done
chmod 755 "$DEPLOY_PATH/litepay"
chmod 440 "$DEPLOY_PATH/config.yaml"
chown 0:10001 "$DEPLOY_PATH/config.yaml"
docker tag "$new_image" litepay:go
# Use a fresh Compose project: otherwise Compose may discover/reuse the renamed
# rollback container by its old service labels instead of creating a new one.
compose_args=(-p "litepay-release-$stamp" -f "$DEPLOY_PATH/docker-compose.yml")
if [[ ${#volumes[@]} == 1 ]]; then
  printf 'volumes:\n  litepay-data:\n    external: true\n    name: %s\n' "${volumes[0]}" > "$stage_dir/volume.yml"
  compose_args+=(-f "$stage_dir/volume.yml")
fi
# Create without starting, so the gate exists before the first application instruction.
(cd "$DEPLOY_PATH" && docker compose "${compose_args[@]}" create --no-build)
if [[ ${#volumes[@]} == 0 ]]; then
  while IFS= read -r volume; do
    [[ -z "$volume" ]] || volumes+=("$volume")
  done < <(docker inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/app/data")}}{{.Name}}{{"\n"}}{{end}}{{end}}' litepay)
fi
[[ ${#volumes[@]} == 1 ]] || { echo 'Cannot identify data volume' >&2; exit 1; }
for volume in "${volumes[@]}"; do
  docker run --rm --user 0:0 --entrypoint /bin/sh -v "$volume:/app/data" "$new_image" \
    -c 'touch /app/data/.deploy-gate && chown -R 10001:10001 /app/data'
  # Existing snapshots are checked before schema migration.
  docker run --rm --read-only --user 10001:10001 --entrypoint /bin/sh -v "$volume:/app/data" "$new_image" \
    -c 'if [ -f /app/data/litepay.db ]; then exec /app/litepay -verify-db /app/data/litepay.db; fi'
done
(cd "$DEPLOY_PATH" && docker compose "${compose_args[@]}" up -d --no-build)
healthy=0
for ((i=0;i<${LP_HEALTH_ATTEMPTS:-30};i++)); do
  if curl -fsS -m 3 http://127.0.0.1:3101/healthz >/dev/null; then
    test "$(docker exec litepay id -u)" = 10001
    docker exec litepay sh -c 'test ! -w / && test -w /app/data'
    healthy=1
    break
  fi
  sleep 2
done
(( healthy == 1 )) || { echo 'New release failed health validation' >&2; exit 1; }
# This is the commit point. After release, never auto-restore an older DB.
docker exec litepay rm /app/data/.deploy-gate
released=1
if (( old_renamed == 1 )); then docker rm "$old_name" >/dev/null; fi
printf 'Release verified. Offline rollback snapshots retained at %s\n' "$backup_dir"
