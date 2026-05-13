#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f data/tuite_tg.db ]]; then
  echo "未找到 data/tuite_tg.db。请先启动一次服务。"
  exit 1
fi

read -r -p "请输入新的后台登录账号 [admin]: " username
username="${username:-admin}"

while true; do
  read -r -p "请输入新的后台登录密码（明文显示）: " password
  if [[ -z "$password" ]]; then
    echo "密码不能为空，请重新输入。"
    continue
  fi
  read -r -p "请再次输入新的后台登录密码（明文显示）: " password2
  if [[ "$password" == "$password2" ]]; then
    break
  fi
  echo "两次密码不一致，请重新输入。"
done

docker compose run --rm \
  -e RESET_ADMIN_USERNAME="$username" \
  -e RESET_ADMIN_PASSWORD="$password" \
  tuite-tg \
  python -c 'import os; from app.auth import get_password_hash; from app.database import init_db, session_scope, set_setting; username=os.environ["RESET_ADMIN_USERNAME"]; password=os.environ["RESET_ADMIN_PASSWORD"]; init_db(); ctx=session_scope(); db=ctx.__enter__(); set_setting(db, "admin_username", username); set_setting(db, "admin_password_hash", get_password_hash(password)); db.flush(); ctx.__exit__(None, None, None); print(f"后台账号密码已重置。账号：{username}")'

docker compose restart tuite-tg
echo "已重启主服务，请使用新账号密码登录。"
