#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

branch="${1:-main}"
remote="${2:-origin}"

echo "正在强制同步 $remote/$branch ..."
git fetch "$remote" "$branch"
git reset --hard "$remote/$branch"

echo "当前版本：$(git rev-parse --short HEAD)"
echo "正在重建并重启服务 ..."
docker compose up -d --build
echo "同步完成。"
