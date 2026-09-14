#!/usr/bin/env bash
# 把打过补丁的 TelegramForwarder 构建并发布到 Docker Hub（amd64 单架构）
#
# 前置：先登录（务必用 Personal Access Token，不是账号密码）
#   docker login -u <你的DockerHub用户名>
#
# 用法：
#   ./publish-dockerhub.sh <DockerHub用户名> [镜像名] [版本]
#   ./publish-dockerhub.sh myname telegramforwarder 1.7.2-enhanced
set -euo pipefail

USERNAME="${1:?用法: $0 <DockerHub用户名> [镜像名] [版本]}"
IMAGE="${2:-telegramforwarder}"
VERSION="${3:-1.7.2-enhanced}"
SRC="${SRC:-.}"                     # 打过补丁的源码目录，默认当前目录
PLATFORM="linux/amd64"

REMOTE="${USERNAME}/${IMAGE}"
GIT_COMMIT="$(git -C "$SRC" rev-parse --short HEAD 2>/dev/null || echo unknown)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "=============================================="
echo " 目标镜像 : docker.io/${REMOTE}"
echo " 标签     : ${VERSION} , latest"
echo " 架构     : ${PLATFORM}"
echo " 源码目录 : ${SRC}"
echo "=============================================="

# ---- 0. 预检 ----
if ! docker info >/dev/null 2>&1; then
  echo "Docker 守护进程未运行"; exit 1
fi
for f in Dockerfile main.py utils/download_manager.py scheduler/message_refresher.py; do
  [ -f "$SRC/$f" ] || { echo "缺少 $SRC/$f —— 补丁打全了吗？"; exit 1; }
done
echo "✓ 源码预检通过"

# 发布前安全扫描：绝不能把 session / 数据库 / .env 打进公共镜像
echo "→ 扫描敏感文件..."
LEAKS="$(find "$SRC" -path '*/.git' -prune -o \( -name '*.session' -o -name '*.db' -o -name '.env' \) -print 2>/dev/null || true)"
if [ -n "$LEAKS" ]; then
  echo "⚠️  构建上下文中存在敏感文件（.dockerignore 应已排除，但仍建议移走）："
  echo "$LEAKS" | sed 's/^/     /'
  read -r -p "   继续构建？(y/N) " yn
  [ "$yn" = "y" ] || exit 1
else
  echo "✓ 无 session / db / .env"
fi

# ---- 1. 登录 ----
if ! docker info --format '{{.IndexServerAddress}}' >/dev/null 2>&1 \
   || ! grep -q "index.docker.io" ~/.docker/config.json 2>/dev/null; then
  echo "→ 未检测到 Docker Hub 登录态，请先执行：docker login -u ${USERNAME}"
  echo "  （密码处填 Personal Access Token，在 Docker Hub → Account Settings → Security 创建）"
  exit 1
fi
echo "✓ 已登录 Docker Hub"

# ---- 2. 构建 ----
echo "→ 构建镜像（amd64）..."
docker build \
  --platform "${PLATFORM}" \
  --label "org.opencontainers.image.title=${IMAGE}" \
  --label "org.opencontainers.image.version=${VERSION}" \
  --label "org.opencontainers.image.revision=${GIT_COMMIT}" \
  --label "org.opencontainers.image.created=${BUILD_DATE}" \
  --label "org.opencontainers.image.source=https://github.com/JoyGanker/TelegramForwarder" \
  --label "org.opencontainers.image.description=TelegramForwarder with download queue, 60s message refresh and pure-text skip" \
  -t "${REMOTE}:${VERSION}" \
  -t "${REMOTE}:latest" \
  "$SRC"

echo "✓ 构建完成"

# ---- 3. 构建后复检（确认镜像里没有敏感文件）----
echo "→ 镜像内敏感文件复检..."
docker run --rm "${REMOTE}:${VERSION}" bash -c '
  n_s=$(find /app -name "*.session" 2>/dev/null | wc -l)
  n_d=$(find /app -name "*.db"      2>/dev/null | wc -l)
  [ -e /app/.env ] && n_e=1 || n_e=0
  echo "    session=$n_s  db=$n_d  .env=$n_e"
  [ "$n_s$n_d$n_e" = "000" ] || { echo "   ⚠️ 镜像内含敏感文件，已中止推送"; exit 1; }
' || { echo "推送已中止"; exit 1; }
echo "✓ 镜像干净"

# ---- 4. 冒烟测试 ----
echo "→ 冒烟测试..."
docker run --rm "${REMOTE}:${VERSION}" python -c "
from utils.download_manager import download_manager as m
from scheduler.message_refresher import message_refresher as r
assert m.max_queue == 250 and m.max_workers == 3 and m.max_concurrent_per_task == 2
assert r.interval == 60 and r.batch_limit == 20
print('   队列', m.max_queue, '| worker', m.max_workers, '| 每任务并发', m.max_concurrent_per_task, '| 刷新', r.interval, 's')
" || { echo "冒烟测试失败，中止推送"; exit 1; }
echo "✓ 功能参数正确"

# ---- 5. 推送 ----
echo "→ 推送 ${REMOTE}:${VERSION}"
docker push "${REMOTE}:${VERSION}"
echo "→ 推送 ${REMOTE}:latest"
docker push "${REMOTE}:latest"

echo
echo "=============================================="
echo " 发布完成"
echo "   docker pull ${REMOTE}:${VERSION}"
echo "   docker pull ${REMOTE}:latest"
echo
echo " 部署端使用：把 docker-compose.yml 里的 image 改为"
echo "   image: ${REMOTE}:${VERSION}"
echo " 然后 docker compose up -d"
echo "=============================================="
