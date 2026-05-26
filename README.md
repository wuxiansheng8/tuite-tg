# Tuite TG

一个把 RSSHub、X/Twitter 正在关注时间线和 Telegram 报警整合在一起的网页后台。

## 设计目标

- 用 RSSHub 把 X/Twitter 的“正在关注 / Following”时间线转成 RSS。
- 支持多个 RSSHub 容器，每个监控账号可选择独立 RSSHub 抓取。
- watcher 全局按秒轮询，按启用账号轮流检查。
- RSSHub 抓取失败时记录异常，并按冷却时间发送 Telegram 报警。

## 服务器部署

```bash
sudo apt update
sudo apt install -y git curl ca-certificates
```

如果服务器还没有 Docker，请先安装 Docker：

```bash
curl -fsSL https://get.docker.com | sudo sh
```

下载并运行中文安装向导：

```bash
cd /opt
sudo git clone https://github.com/wuxiansheng8/tuite-tg.git
sudo chown -R $USER:$USER /opt/tuite-tg
cd /opt/tuite-tg
chmod +x scripts/install.sh
./scripts/install.sh
```

> 必须先运行安装向导。这个项目只保留这一条正式部署路径。

安装向导会要求输入：

- 网页访问端口
- 后台登录账号
- 后台登录密码

安装器会把后台账号密码写入数据库，并显示 `后台密码校验：True` 和 `HTTP 登录自检：True`。如果没有看到这两行 `True`，请不要继续使用，先检查安装输出。

Telegram、全局轮询秒数、失败冷却分钟等配置可以启动后在网页后台修改。

安装完成后打开：

```text
http://服务器IP:你输入的端口
```

常用命令：

```bash
docker compose ps
docker compose logs -f tuite-tg
docker compose down
```

强制更新到 GitHub 最新版：

```bash
chmod +x scripts/update.sh
./scripts/update.sh
```

> `scripts/update.sh` 会丢弃服务器项目目录里的本地改动，并强制同步到 GitHub 最新 `main` 分支。服务器上不要直接改项目文件。

重置后台账号密码：

```bash
chmod +x scripts/reset-admin.sh
./scripts/reset-admin.sh
```

> 注意：第一次启动时，后台账号密码会写入 `data/tuite_tg.db`。如果后面只改 `.env` 里的 `WEB_PASSWORD`，不会自动修改已有数据库里的登录密码。需要重置时可以停止服务并删除数据库后重新运行安装向导。

```bash
docker compose down
rm -f data/tuite_tg.db
./scripts/install.sh
```

## RSSHub 实例建议

推荐按账号或代理拆分 RSSHub 容器，全部在网页后台 `RSSHub方案 -> RSSHub 容器` 里新增、编辑、删除：

```text
rsshub1 -> http://rsshub1:1200 -> auth_token1 + proxy1
rsshub2 -> http://rsshub2:1200 -> auth_token2 + proxy2
...
rsshub10 -> http://rsshub10:1200 -> auth_token10 + proxy10
```

`docker-compose.yml` 只保留主程序，不再固定写死 `rsshub1`、`rsshub2`。这样重启服务后，不会把网页里删除或改名的 RSSHub 容器重新拉回来。
RSSHub 当前文档里 Twitter Home latest timeline 路由用于抓取“正在关注 / Following”时间线。项目实际请求的路由是：

```text
/twitter/home_latest/count=100&includeRts=true&showQuotedInTitle=true
```

RSSHub 的 Twitter 路由需要 `TWITTER_AUTH_TOKEN`，部分部署还会用到 `TWITTER_THIRD_PARTY_API`，所以这些值也在网页新增/编辑 RSSHub 时填写。

后台里新增监控账号时：

```text
账号备注：可选
账号名或标识：例如 5号手机
RSSHub：选择 rsshub1/rsshub2，默认使用第一个 RSSHub 容器
```

代理设置里修改代理地址后，会自动同步到正在使用旧地址的 RSSHub 配置；如果 RSSHub 容器是网页创建和管理的，会同时尝试按新代理重建容器，让 `PROXY_URI` 生效。正在使用中的代理不能直接停用，需要先把 RSSHub 切到其它代理。

## 抓取异常时的处理

当 RSSHub 返回 HTTP 错误、RSS 解析失败或其它抓取异常时：

1. Tuite TG 记录该账号时间线的失败状态。
2. 按冷却时间控制 TG 报警频率，避免重复刷屏。
3. 下一轮仍会继续扫描其它启用账号；单个账号失败不会拖停整个监控。

## 预判风险

- X/Twitter 可能改返回结构、认证要求或风控策略，导致 RSSHub 抓取波动。
- “正在关注 / Following”是账号自己的时间线。即使多个账号关注完全一致，X 也可能因账号状态、代理/IP、风控、地区和缓存返回不同数量或排序。
- 5 秒是全局轮询，不建议监控账号过多时设置过低。
- RSSHub 的 `CACHE_EXPIRE` 如果太大，watcher 会反复拿缓存；建议 30 秒起步。
- 代理质量会直接影响 RSSHub 抓取稳定性。
- RSSHub、代理和 Telegram 凭据保存在本地 SQLite，后台不要裸露公网。

## 下一步可增强

- RSSHub 健康检查和镜像版本提示。
- Telegram 按失败类型分级报警。
- Redis 去重，方便多进程部署。
- 把代理加密存储。
