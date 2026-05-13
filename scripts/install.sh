#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

say() {
  printf '%s\n' "$1"
}

say_error() {
  printf '%s\n' "$1" >&2
}

ask() {
  local prompt="$1"
  local default_value="${2:-}"
  local value
  if [[ -n "$default_value" ]]; then
    read -r -p "$prompt [$default_value]: " value
    printf '%s' "${value:-$default_value}"
  else
    read -r -p "$prompt: " value
    printf '%s' "$value"
  fi
}

ask_required() {
  local prompt="$1"
  local default_value="${2:-}"
  local value
  while true; do
    value="$(ask "$prompt" "$default_value")"
    if [[ -n "$value" ]]; then
      printf '%s' "$value"
      return
    fi
    say_error "不能为空，请重新输入。"
  done
}

ask_password() {
  local first second
  while true; do
    read -r -p "请输入后台登录密码（明文显示）: " first
    if [[ -z "$first" ]]; then
      say_error "密码不能为空，请重新输入。"
      continue
    fi
    read -r -p "请再次输入后台登录密码（明文显示）: " second
    if [[ "$first" == "$second" ]]; then
      printf '%s' "$first"
      return
    fi
    say_error "两次密码不一致，请重新输入。"
  done
}

read_web_username() {
  while true; do
    read -r -p "请输入后台登录账号 [admin]: " WEB_USERNAME
    WEB_USERNAME="${WEB_USERNAME:-admin}"
    if [[ -n "$WEB_USERNAME" ]]; then
      return
    fi
    say_error "账号不能为空，请重新输入。"
  done
}

read_web_password() {
  local first second
  while true; do
    read -r -p "请输入后台登录密码（明文显示）: " first
    if [[ -z "$first" ]]; then
      say_error "密码不能为空，请重新输入。"
      continue
    fi
    read -r -p "请再次输入后台登录密码（明文显示）: " second
    if [[ "$first" == "$second" ]]; then
      WEB_PASSWORD="$first"
      return
    fi
    say_error "两次密码不一致，请重新输入。"
  done
}

validate_port() {
  local port="$1"
  [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1 && port <= 65535 ))
}

require_command() {
  local command_name="$1"
  local install_hint="$2"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    say "未检测到 $command_name。"
    say "$install_hint"
    exit 1
  fi
}

generate_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
  else
    date +%s%N | sha256sum | awk '{print $1}'
  fi
}

env_quote() {
  local value="$1"
  value=${value//\'/\'\\\'\'}
  printf "'%s'" "$value"
}

write_env() {
  local env_file="$1"
  cat > "$env_file" <<EOF
WEB_PORT=$WEB_PORT
WEB_USERNAME=$(env_quote "$WEB_USERNAME")
WEB_PASSWORD=$(env_quote "$WEB_PASSWORD")
TUITE_TG_SECRET_KEY=$TUITE_TG_SECRET_KEY
GLOBAL_POLL_SECONDS=$GLOBAL_POLL_SECONDS
FAILURE_COOLDOWN_MINUTES=$FAILURE_COOLDOWN_MINUTES
TELEGRAM_BOT_TOKEN=$(env_quote "$TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID=$(env_quote "$TELEGRAM_CHAT_ID")
APPRISE_URLS=$(env_quote "$APPRISE_URLS")
EOF
}

write_install_marker() {
  mkdir -p data
  cat > "data/install_wizard_state.json" <<EOF
{
  "completed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "web_port": "$WEB_PORT",
  "web_username": "$WEB_USERNAME"
}
EOF
}

sync_admin_credentials() {
  local username="$1"
  local password="$2"
  if [[ -z "$username" || -z "$password" ]]; then
    say_error "后台账号或密码为空，不能写入数据库。"
    return 1
  fi
  docker compose run --rm --no-deps \
    tuite-tg \
    python -c 'import os; from app.auth import get_password_hash, verify_password; from app.database import init_db, session_scope, set_setting, get_setting; username=os.environ.get("WEB_USERNAME", ""); password=os.environ.get("WEB_PASSWORD", ""); init_db(); ctx=session_scope(); db=ctx.__enter__(); set_setting(db, "admin_username", username); set_setting(db, "admin_password_hash", get_password_hash(password)); db.flush(); stored=get_setting(db, "admin_username", ""); stored_hash=get_setting(db, "admin_password_hash", ""); ok=bool(username) and bool(password) and stored == username and verify_password(password, stored_hash); ctx.__exit__(None, None, None); print(f"容器读取 WEB_USERNAME：{username}"); print(f"数据库中的后台账号已写入：{stored}"); print(f"后台密码校验：{ok}"); raise SystemExit(0 if ok else 1)'
}

wait_for_http() {
  local url="$1"
  local attempt
  for attempt in $(seq 1 30); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  say_error "服务启动后没有通过健康检查：$url"
  return 1
}

verify_http_login() {
  local username="$1"
  local password="$2"
  local url="http://127.0.0.1:$WEB_PORT/api/token"
  local status
  status="$(
    curl -sS -o /tmp/tuite_tg_login_check.json -w "%{http_code}" \
      -X POST \
      --data-urlencode "username=$username" \
      --data-urlencode "password=$password" \
      "$url"
  )"
  if [[ "$status" != "200" ]]; then
    say_error "HTTP 登录自检失败，状态码：$status"
    say_error "接口返回：$(cat /tmp/tuite_tg_login_check.json 2>/dev/null || true)"
    return 1
  fi
  say "HTTP 登录自检：True"
}

say "========================================"
say " Tuite TG Ubuntu 安装向导"
say "========================================"
say "本向导会生成 .env 配置文件，并使用 Docker Compose 启动服务。"
say ""

require_command "docker" "请先安装 Docker：curl -fsSL https://get.docker.com | sudo sh"
if ! docker compose version >/dev/null 2>&1; then
  say "未检测到 Docker Compose 插件。请先安装 docker-compose-plugin。"
  exit 1
fi

if [[ -f .env ]]; then
  say "检测到已有 .env 配置。"
  read -r -p "是否覆盖并重新配置？输入 yes 覆盖，其他输入取消: " overwrite
  if [[ "$overwrite" != "yes" ]]; then
    say "已取消安装向导，现有 .env 未修改。"
    exit 0
  fi
  cp .env ".env.bak.$(date +%Y%m%d%H%M%S)"
  say "已备份旧配置。"
fi

while true; do
  WEB_PORT="$(ask "请输入网页访问端口" "8000")"
  if validate_port "$WEB_PORT"; then
    break
  fi
  say_error "端口号必须是 1-65535 之间的数字。"
done

read_web_username
read_web_password
if [[ -z "$WEB_USERNAME" || -z "$WEB_PASSWORD" ]]; then
  say_error "后台账号或密码为空，安装已停止。"
  exit 1
fi
say "后台登录账号确认：$WEB_USERNAME"
TUITE_TG_SECRET_KEY="$(generate_secret)"
GLOBAL_POLL_SECONDS="5"
FAILURE_COOLDOWN_MINUTES="10"
TELEGRAM_BOT_TOKEN=""
TELEGRAM_CHAT_ID=""
APPRISE_URLS=""

write_env ".env"
write_install_marker

say ""
say "配置已写入 .env，并生成安装完成标记。"
say "正在停止旧服务..."
docker compose down --remove-orphans
say "正在构建镜像..."
docker compose build
say "正在写入并确认后台账号密码..."
sync_admin_credentials "$WEB_USERNAME" "$WEB_PASSWORD"
say "正在启动服务..."
docker compose up -d
say "正在等待服务健康检查..."
wait_for_http "http://127.0.0.1:$WEB_PORT/health"
say "正在验证 HTTP 登录接口..."
verify_http_login "$WEB_USERNAME" "$WEB_PASSWORD"

say ""
say "安装完成。"
say "后台地址：http://服务器IP:$WEB_PORT"
say "登录账号：$WEB_USERNAME"
say "登录密码：你刚才输入的密码"
say ""
say "常用命令："
say "查看状态：docker compose ps"
say "查看日志：docker compose logs -f tuite-tg"
say "停止服务：docker compose down"
