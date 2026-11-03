#!/usr/bin/env bash
# =============================================================================
# LitePay 一键部署
#   1. 交叉编译 linux/arm64 静态二进制
#   2. 打包（二进制 + 启动配置 + compose）
#   3. 远端解包校验并预构建镜像
#   4. 构建成功后切换容器，保留业务数据卷
#   5. 健康验证后放行业务，失败恢复停机一致性快照，保留备份
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
DEPLOY_ARCH=${DEPLOY_ARCH:-arm64}
case "$DEPLOY_ARCH" in amd64|arm64) ;; *) echo 'DEPLOY_ARCH must be amd64 or arm64' >&2; exit 1;; esac
DEPLOY_BINARY="dist/litepay-linux-$DEPLOY_ARCH"
CGO_ENABLED=0 GOOS=linux GOARCH="$DEPLOY_ARCH" go build -trimpath -ldflags "-s -w" -o "$DEPLOY_BINARY" .
# shellcheck disable=SC2012 # This is one known fixed path, not an arbitrary directory listing.
ls -lh "$DEPLOY_BINARY" | awk '{print "   " $9 "  " $5}'

echo "▶ [2/5] 打包 ..."
cp "$DEPLOY_BINARY" "$STAGE/litepay"
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

echo "▶ [3/5] 验证、切换与失败回滚 ..."
"${SSH_BIN[@]}" "$TARGET" "DEPLOY_PATH='$DEPLOY_PATH' ARCHIVE='$REMOTE_ARCHIVE' bash -s" < scripts/deploy-activate.sh

echo "▶ [4/5] 新版本健康检查已通过，业务已放行"

echo "▶ [5/5] 公网检查（保留恢复快照）..."
"${SSH_BIN[@]}" "$TARGET" "curl -fsS -m 10 -o /dev/null -w '   ✓ https://$DEPLOY_DOMAIN/healthz → HTTP %{http_code}\n' https://$DEPLOY_DOMAIN/healthz" \
  || echo "   ! 公网访问失败（检查 DNS / Caddy / 证书；请勿盲目恢复旧数据库）"

echo ""
echo "✅ 部署完成"
echo "   站点   : https://$DEPLOY_DOMAIN"
echo "   后台   : https://$DEPLOY_DOMAIN/admin"
echo "   目录   : $DEPLOY_PATH"
echo "   备份   : 保留在部署目录旁的 -backups 目录"
