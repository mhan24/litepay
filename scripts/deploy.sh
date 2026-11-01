#!/usr/bin/env bash
# =============================================================================
# LitePay 一键部署
#   1. 交叉编译 linux/arm64 静态二进制
#   2. 打包（二进制 + 启动配置 + compose）
#   3. 远端解包校验并预构建镜像
#   4. 构建成功后切换容器，保留业务数据卷
#   5. 健康检查并删除历史回滚归档（开发状态不保留备份）
#
# 用法：./scripts/deploy.sh
# 依赖：部署环境中的 DEPLOY_HOST / DEPLOY_USER / DEPLOY_PASSWORD（或改用 SSH Key）
# =============================================================================
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

# 部署凭据来源，优先级：
#   1) LP_ENV_FILE 指定的文件
#   2) 当前 shell 已导出的变量（LP_SKIP_ENV=1 时完全跳过读取文件）
#   3) 项目根目录的 .env（仅用于部署凭据）
if [[ -n "${LP_ENV_FILE:-}" && -f "${LP_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$LP_ENV_FILE"
  set +a
elif [[ -z "${LP_SKIP_ENV:-}" && -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

: "${DEPLOY_HOST:?缺少 DEPLOY_HOST}"
: "${DEPLOY_USER:?缺少 DEPLOY_USER}"
: "${DEPLOY_PORT:=22}"
: "${DEPLOY_PATH:=/opt/litepay}"
: "${DEPLOY_DOMAIN:=pay.9527.tr}"

[[ ${#DEPLOY_HOST} -le 253 && "$DEPLOY_HOST" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] \
  || { echo '非法 DEPLOY_HOST' >&2; exit 1; }
[[ ${#DEPLOY_USER} -le 64 && "$DEPLOY_USER" =~ ^[A-Za-z_][A-Za-z0-9._-]*$ ]] \
  || { echo '非法 DEPLOY_USER' >&2; exit 1; }
if [[ ! "$DEPLOY_PORT" =~ ^[0-9]{1,5}$ ]] || (( DEPLOY_PORT < 1 || DEPLOY_PORT > 65535 )); then
  echo '非法 DEPLOY_PORT' >&2
  exit 1
fi

SSH_COMMON=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 -p "$DEPLOY_PORT")
SCP_COMMON=(-o StrictHostKeyChecking=accept-new -P "$DEPLOY_PORT")

if [[ -n "${DEPLOY_PASSWORD:-}" ]]; then
  command -v sshpass >/dev/null 2>&1 || {
    echo "✗ DEPLOY_PASSWORD 已配置，但本机缺少 sshpass" >&2
    exit 1
  }
  export SSHPASS="$DEPLOY_PASSWORD"
  SSH_BIN=(sshpass -e ssh "${SSH_COMMON[@]}")
  SCP_BIN=(sshpass -e scp "${SCP_COMMON[@]}")
else
  SSH_BIN=(ssh "${SSH_COMMON[@]}")
  SCP_BIN=(scp "${SCP_COMMON[@]}")
fi

[[ "$DEPLOY_PATH" =~ ^/[A-Za-z0-9_./-]+$ && "$DEPLOY_PATH" != / ]] || { echo '非法 DEPLOY_PATH' >&2; exit 1; }
[[ ${#DEPLOY_DOMAIN} -le 253 && "$DEPLOY_DOMAIN" =~ ^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$ ]] \
  || { echo '非法 DEPLOY_DOMAIN' >&2; exit 1; }
TARGET="$DEPLOY_USER@$DEPLOY_HOST"

if [[ -z "${ADMIN_PASSWORD:-}" ]]; then
  echo "✗ 缺少 ADMIN_PASSWORD（部署时写入服务器配置，不会进入应用环境）" >&2
  exit 1
fi
ADMIN_USERNAME=${ADMIN_USERNAME:-admin}
[[ "$ADMIN_USERNAME" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || { echo '非法 ADMIN_USERNAME' >&2; exit 1; }
[[ ${#ADMIN_PASSWORD} -ge 6 && ${#ADMIN_PASSWORD} -le 1024 ]] \
  || { echo 'ADMIN_PASSWORD 长度需为 6~1024' >&2; exit 1; }

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

echo "▶ [1/5] 编译 linux/arm64 ..."
mkdir -p dist
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o dist/litepay-linux-arm64 .
# shellcheck disable=SC2012 # This is one known fixed path, not an arbitrary directory listing.
ls -lh dist/litepay-linux-arm64 | awk '{print "   " $9 "  " $5}'

echo "▶ [2/5] 打包 ..."
cp dist/litepay-linux-arm64 "$STAGE/litepay"
cp deploy/config.yaml "$STAGE/config.yaml"
ADMIN_USERNAME="$ADMIN_USERNAME" ADMIN_PASSWORD="$ADMIN_PASSWORD" perl -MJSON::PP -0pi -e '
  s/^(\s*username:\s*).*$/"$1" . encode_json($ENV{ADMIN_USERNAME})/me;
  s/^(\s*password:\s*).*$/"$1" . encode_json($ENV{ADMIN_PASSWORD})/me;
' "$STAGE/config.yaml"
cp deploy/Dockerfile "$STAGE/Dockerfile"
cp deploy/docker-compose.yml "$STAGE/docker-compose.yml"
cp .dockerignore "$STAGE/.dockerignore"
# macOS 的 bsdtar 会额外打包 ._ 元数据文件，COPYFILE_DISABLE 可关闭该行为
COPYFILE_DISABLE=1 tar --no-xattrs -czf "$STAGE/pkg.tar.gz" -C "$STAGE" \
  litepay config.yaml Dockerfile docker-compose.yml .dockerignore

REMOTE_ARCHIVE=$("${SSH_BIN[@]}" "$TARGET" 'umask 077; mktemp /tmp/litepay-deploy.XXXXXXXX')
[[ "$REMOTE_ARCHIVE" =~ ^/tmp/litepay-deploy\.[A-Za-z0-9]+$ ]] || { echo 'Invalid remote archive path' >&2; exit 1; }
"${SCP_BIN[@]}" "$STAGE/pkg.tar.gz" "$TARGET:$REMOTE_ARCHIVE" >/dev/null
echo "   ✓ 已上传"

echo "▶ [3/5] 校验、预构建并切换 ..."
"${SSH_BIN[@]}" "$TARGET" "DEPLOY_PATH='$DEPLOY_PATH' ARCHIVE='$REMOTE_ARCHIVE' bash -s" <<'REMOTE'
set -euo pipefail
umask 077
STAGE_DIR=""
trap 'rm -f "$ARCHIVE"; [ -z "$STAGE_DIR" ] || rm -rf "$STAGE_DIR"' EXIT

if ! tar tzf "$ARCHIVE" >/dev/null; then
  echo "   ✗ 上传包损坏，停止部署"
  exit 1
fi

STAGE_DIR=$(mktemp -d /tmp/litepay-stage.XXXXXXXX)
tar xzf "$ARCHIVE" -C "$STAGE_DIR"
chmod 640 "$STAGE_DIR/config.yaml"
chmod +x "$STAGE_DIR/litepay"

echo "   ✓ 上传包校验通过"
(cd "$STAGE_DIR" && docker compose config >/dev/null)
docker build -t litepay:go "$STAGE_DIR" >/dev/null
echo "   ✓ 新镜像预构建完成"

volume_names=()
if docker inspect litepay >/dev/null 2>&1; then
  mounts=()
  while IFS= read -r mount; do
    [ -n "$mount" ] && mounts+=("$mount")
  done < <(docker inspect -f '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}|{{.Destination}}{{"\n"}}{{end}}{{end}}' litepay)
  if [ "${#mounts[@]}" -eq 0 ]; then
    echo "   ✗ 无法解析 litepay 的命名卷挂载，停止部署"
    exit 1
  fi
  for mount in "${mounts[@]}"; do
    volume_names+=("${mount%%|*}")
  done
fi

mkdir -p "$DEPLOY_PATH"
install -m 0440 "$STAGE_DIR/config.yaml" "$DEPLOY_PATH/config.yaml"
chown 0:10001 "$DEPLOY_PATH/config.yaml"
install -m 755 "$STAGE_DIR/litepay" "$DEPLOY_PATH/litepay"
install -m 644 "$STAGE_DIR/Dockerfile" "$DEPLOY_PATH/Dockerfile"
install -m 644 "$STAGE_DIR/docker-compose.yml" "$DEPLOY_PATH/docker-compose.yml"
install -m 644 "$STAGE_DIR/.dockerignore" "$DEPLOY_PATH/.dockerignore"
rm -f "$ARCHIVE"
echo "   ✓ 新版本已就位 → $DEPLOY_PATH"

for name in "${volume_names[@]}"; do
  docker volume inspect "$name" >/dev/null 2>&1 || {
    echo "   ✗ 业务数据卷 $name 在部署前丢失，停止启动"
    exit 1
  }
done

if docker inspect litepay >/dev/null 2>&1; then
  docker stop litepay >/dev/null
  docker rm litepay >/dev/null
  echo "   ✓ 旧容器已停止并删除"
fi

# 数据卷属主修正必须在旧容器停止后执行，避免在 SQLite 仍在写入时
# 递归改动文件元数据。一次性容器只授予完成 chown 所需的权限。
for name in "${volume_names[@]}"; do
  docker run --rm --read-only --user 0:0 \
    --cap-drop ALL --cap-add CHOWN --cap-add DAC_OVERRIDE \
    --security-opt no-new-privileges:true \
    --entrypoint /bin/sh -v "$name:/app/data" litepay:go \
    -c 'chown -R 10001:10001 /app/data'
  echo "   ✓ 已保留并修正业务数据卷 $name 属主"
done

(cd "$DEPLOY_PATH" && docker compose up -d --no-build)
REMOTE

echo "▶ [4/5] 健康检查 ..."
# shellcheck disable=SC2016 # The remote shell must expand its own loop variable.
"${SSH_BIN[@]}" "$TARGET" '
for i in $(seq 1 30); do
  if curl -fsS -m 3 http://127.0.0.1:3101/healthz >/tmp/lp_hz.json 2>/dev/null; then
    echo "   ✓ 容器健康：$(cat /tmp/lp_hz.json)"
    test "$(docker exec litepay id -u)" = "10001" || { echo "   ✗ 容器未以 UID 10001 运行"; exit 1; }
    docker exec litepay sh -c "test ! -w / && test -w /app/data" \
      || { echo "   ✗ 根文件系统或数据卷权限不符合预期"; exit 1; }
    echo "   ✓ 非 root、只读根文件系统、数据卷可写"
    exit 0
  fi
  sleep 2
done
echo "   ✗ 健康检查失败，最近日志："
docker logs --tail 40 litepay 2>&1 || true
exit 1
'

echo "▶ [5/5] 公网检查并清理历史回滚归档 ..."
"${SSH_BIN[@]}" "$TARGET" "curl -fsS -m 10 -o /dev/null -w '   ✓ https://$DEPLOY_DOMAIN/healthz → HTTP %{http_code}\n' https://$DEPLOY_DOMAIN/healthz" \
  || echo "   ! 公网访问失败（检查 DNS / Caddy / 证书）"
"${SSH_BIN[@]}" "$TARGET" '
if [ -d /opt/backups ]; then
  find /opt/backups -maxdepth 1 -type f \
    \( -name "litepay-old-*.tar.gz" -o -name "litepay-volume-*.tar.gz" \) -delete
  echo "   ✓ 已删除历史 LitePay 回滚归档"
fi
'

echo ""
echo "✅ 部署完成"
echo "   站点   : https://$DEPLOY_DOMAIN"
echo "   后台   : https://$DEPLOY_DOMAIN/admin"
echo "   目录   : $DEPLOY_PATH"
echo "   备份   : 已按开发模式关闭"
