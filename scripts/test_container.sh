#!/bin/sh
# Exercise the exact image startup chain with networking disabled and disposable data.
set -eu
image=${1:-litepay:ci}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
task_tmp=$(mktemp -d)
name="litepay-container-check-$$"
trap 'docker rm -f "$name" "$name-default" >/dev/null 2>&1 || true; rm -rf "$task_tmp"' EXIT HUP INT TERM
docker run -d --name "$name-default" --network none "$image" >/dev/null
attempt=0
while [ "$attempt" -lt 15 ] && [ "$(docker inspect -f '{{.State.Running}}' "$name-default")" = true ]; do
    attempt=$((attempt+1))
    sleep 1
done
if [ "$(docker inspect -f '{{.State.Running}}' "$name-default")" = true ]; then
    echo 'unconfigured production image unexpectedly stayed running' >&2
    exit 1
fi
[ "$(docker inspect -f '{{.State.ExitCode}}' "$name-default")" != 0 ]
docker logs "$name-default" >"$task_tmp/default.log" 2>&1
grep -q 'production 模式要求 HTTPS' "$task_tmp/default.log"
sed -e 's/base_url: ""/base_url: "https:\/\/pay.example.com"/' -e 's/notify_hosts: \[\]/notify_hosts: ["shop.example.com"]/' "$root/config.production.yaml" >"$task_tmp/config.yaml"
chmod 755 "$task_tmp"
chmod 644 "$task_tmp/config.yaml"
docker run -d --name "$name" --network none --read-only --user 10001:10001 --cap-drop ALL --security-opt no-new-privileges:true --tmpfs /tmp:size=16m,mode=1777 --tmpfs /app/data:uid=10001,gid=10001,mode=0700 -v "$task_tmp/config.yaml:/app/config.yaml:ro" "$image" >/dev/null
attempt=0
while [ "$attempt" -lt 30 ]; do
    if docker exec "$name" wget -qO- http://127.0.0.1:8080/healthz >"$task_tmp/health.json"; then
        grep -q '"ok":true' "$task_tmp/health.json"
        echo 'PASS: default image refuses unconfigured production; explicit HTTPS configuration starts as UID 10001'
        exit 0
    fi
    attempt=$((attempt+1))
    sleep 1
done
docker logs "$name"
exit 1
