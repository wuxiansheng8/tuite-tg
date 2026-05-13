from __future__ import annotations

import asyncio
import hashlib
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urljoin

import feedparser
import httpx
from sqlalchemy.orm import Session

from .database import (
    RsshubInstance,
    SeenItem,
    UserAlias,
    WatchList,
    WatchListBinding,
    add_log,
    get_setting,
    session_scope,
    set_setting,
    utc_now,
)
from .notifier import format_alert, format_feed_item, send_apprise, send_telegram, split_rsshub_description
from .openai_client import OpenAIConfigError, OpenAIRequestError, build_endpoint, translate_text


class Watcher:
    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self._lock = asyncio.Lock()
        self._cursor = 0

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            await self._task

    async def trigger_once(self) -> None:
        async with self._lock:
            await self.run_once()

    async def check_list(self, list_row_id: int) -> None:
        async with self._lock:
            await self.poll_list_by_watch_list(list_row_id)

    async def check_pair(self, token_id: int, list_row_id: int) -> None:
        await self.check_list(list_row_id)

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                async with self._lock:
                    await self.run_once()
            except Exception as exc:
                with session_scope() as db:
                    add_log(db, "ERROR", f"watcher 主循环异常: {exc}")
            interval = read_int_setting("global_poll_seconds", 5)
            jitter = random.uniform(0.2, 1.5)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=max(1, interval) + jitter)
            except asyncio.TimeoutError:
                pass

    async def run_once(self) -> None:
        with session_scope() as db:
            binding_id = self._next_binding(db)
            if not binding_id:
                return

        await self.poll_binding(binding_id)

    def _next_binding(self, db: Session) -> Optional[int]:
        bindings = (
            db.query(WatchListBinding)
            .join(WatchList, WatchList.id == WatchListBinding.watch_list_id)
            .filter(
                WatchList.token_id == 0,
                WatchList.enabled.is_(True),
                WatchListBinding.enabled.is_(True),
            )
            .order_by(WatchListBinding.id.asc())
            .all()
        )
        if not bindings:
            return None
        self._cursor = self._cursor % len(bindings)
        binding_id = bindings[self._cursor].id
        self._cursor += 1
        return int(binding_id)

    async def poll_pair(self, token_id: int, list_row_id: int) -> None:
        await self.poll_list_by_watch_list(list_row_id)

    async def poll_list_by_watch_list(self, list_row_id: int) -> None:
        with session_scope() as db:
            binding = (
                db.query(WatchListBinding)
                .filter(WatchListBinding.watch_list_id == list_row_id, WatchListBinding.enabled.is_(True))
                .order_by(WatchListBinding.id.asc())
                .first()
            )
            if not binding:
                return
        await self.poll_binding(binding.id)

    async def poll_binding(self, binding_id: int) -> None:
        with session_scope() as db:
            binding = db.query(WatchListBinding).filter(WatchListBinding.id == binding_id).first()
            if not binding:
                return
            watch_list = db.query(WatchList).filter(WatchList.id == binding.watch_list_id, WatchList.token_id == 0).first()
            if not watch_list:
                return
            rsshub = db.query(RsshubInstance).filter(RsshubInstance.id == binding.rsshub_instance_id).first()
            if not rsshub:
                mark_binding_failure(db, binding, watch_list, "没有可用的 RSSHub 容器，请先创建 RSSHub，或重新绑定 RSSHub。")
                return
            source_snapshot = snapshot_rsshub(rsshub, binding.id)
            list_snapshot = snapshot_list(watch_list)
            bootstrap = (
                db.query(SeenItem)
                .filter(SeenItem.list_id == watch_list.list_id)
                .first()
                is None
            )

        try:
            items = await fetch_rss_items(source_snapshot, list_snapshot)
            await self.process_items(source_snapshot, list_snapshot, items, bootstrap)
            await mark_binding_success(source_snapshot, list_snapshot, len(items))
        except Exception as exc:
            await self.handle_source_failure(source_snapshot, list_snapshot, str(exc))

    async def process_items(
        self,
        token: dict,
        watch_list: dict,
        items: list[dict],
        bootstrap: bool,
    ) -> None:
        bot_token, chat_id, apprise_urls = read_notify_settings()
        for item in reversed(items):
            item_id = normalize_item_id(item)
            candidate_ids = candidate_item_ids(item)
            title = item.get("title", "")
            description = item.get("description", "")
            link = item.get("link", "")
            username = normalize_username(str(item.get("username") or extract_username_from_item(item)))
            author_label = resolve_author_label(username)
            with session_scope() as db:
                exists = db.query(SeenItem).filter(SeenItem.item_id.in_(candidate_ids)).first()
                if exists:
                    continue
                seen = SeenItem(
                    item_id=item_id,
                    list_id=watch_list["list_id"],
                    token_id=token["id"],
                    title=title,
                    link=link,
                    forwarded_at=None if bootstrap else utc_now(),
                )
                db.add(seen)
                if bootstrap:
                    add_log(db, "INFO", f"首次启动记录历史推文: {item_id}")
                    continue
                add_log(db, "INFO", f"发现新推文: {item_id}")

            outer_text, quote_text = split_rsshub_description(description)
            retweet_source, outer_text = extract_retweet_source(outer_text)
            quote_source, quote_text = extract_quote_source(quote_text)
            is_retweet = bool(retweet_source) or is_retweet_text(outer_text or title)
            translated_outer = await maybe_translate_title(outer_text or title)
            translated_quote = await maybe_translate_title(quote_text) if quote_text else ""
            message = format_feed_item(
                author_label=author_label,
                translated_outer=translated_outer,
                translated_quote=translated_quote,
                is_retweet=is_retweet,
                retweet_source=retweet_source,
                quote_source=quote_source,
            )
            try:
                await send_telegram_with_retry(
                    bot_token,
                    chat_id,
                    message,
                    f"推文 {item_id}",
                    button_text="查看原文",
                    button_url=link,
                )
                with session_scope() as db:
                    add_log(db, "INFO", f"TG 推送成功: {item_id}")
                if apprise_urls:
                    prefix = f"{author_label}\n" if author_label else ""
                    send_apprise(apprise_urls, "X List 更新", f"{prefix}{title}\n{link}")
                    with session_scope() as db:
                        add_log(db, "INFO", f"Apprise 推送成功: {item_id}")
            except Exception as exc:
                with session_scope() as db:
                    add_log(db, "ERROR", f"推送失败 {item_id}: {exc}")

    async def handle_source_failure(self, token: dict, watch_list: dict, error: str) -> None:
        bot_token, chat_id, _ = read_notify_settings()
        title = "X/RSSHub 抓取异常"
        body = f"{token['name']} / List {watch_list['list_id']} 抓取失败。"
        should_alert = True
        with session_scope() as db:
            row = db.query(WatchListBinding).filter(WatchListBinding.id == token["binding_id"]).first()
            if row:
                now = utc_now()
                was_healthy = row.healthy
                row.healthy = False
                row.last_error = error[:2000]
                row.last_checked_at = now
                alert_interval = timedelta(minutes=read_int_setting("failure_cooldown_minutes", 10))
                should_alert = (
                    was_healthy
                    or row.last_alerted_at is None
                    or elapsed_since(row.last_alerted_at, now) >= alert_interval
                )
                if should_alert:
                    row.last_alerted_at = now
            add_log(db, "ERROR", f"{body} 原因: {error}")
        if should_alert:
            await notify_safely(bot_token, chat_id, format_alert(title, body, error))


async def fetch_rss_items(token: dict, watch_list: dict) -> list[dict]:
    base = token["rsshub_url"].rstrip("/") + "/"
    url = urljoin(base, f"twitter/list/{watch_list['list_id']}")
    retry_statuses = {502, 503, 504}
    last_resp: httpx.Response | None = None
    async with httpx.AsyncClient(timeout=35.0) as client:
        for attempt in range(1, 4):
            resp = await client.get(url)
            last_resp = resp
            if resp.status_code not in retry_statuses:
                break
            if attempt < 3:
                await asyncio.sleep(2 * attempt)
    resp = last_resp
    if resp is None:
        raise RuntimeError("RSSHub 未返回响应")
    if resp.status_code >= 400:
        raise RuntimeError(f"RSSHub HTTP {resp.status_code}: {resp.text[:300]}")
    parsed = feedparser.parse(resp.text)
    if parsed.bozo:
        raise RuntimeError(f"RSS 解析失败: {parsed.bozo_exception}")
    entries = []
    for entry in parsed.entries:
        description = (
            entry.get("summary")
            or entry.get("description")
            or next(
                (
                    item.get("value", "")
                    for item in entry.get("content", [])
                    if isinstance(item, dict) and item.get("value")
                ),
                "",
            )
        )
        entries.append(
            {
                "id": entry.get("id") or entry.get("guid") or entry.get("link"),
                "title": entry.get("title", ""),
                "description": description,
                "link": entry.get("link", ""),
                "username": extract_username_from_entry(entry),
            }
        )
    return entries


async def mark_binding_success(source: dict, watch_list: dict, item_count: int) -> None:
    bot_token, chat_id, _ = read_notify_settings()
    recovered = False
    with session_scope() as db:
        row = db.query(WatchListBinding).filter(WatchListBinding.id == source["binding_id"]).first()
        if row:
            recovered = not row.healthy or bool(row.last_error)
            row.healthy = True
            row.last_error = ""
            row.last_checked_at = utc_now()
            row.last_success_at = utc_now()
        parent = db.query(WatchList).filter(WatchList.id == watch_list["id"]).first()
        if parent:
            parent.healthy = True
            parent.last_error = ""
            parent.last_checked_at = utc_now()
            parent.last_success_at = utc_now()
        add_log(db, "INFO", f"{source['name']} / List {watch_list['list_id']} 检查完成，返回 {item_count} 条")
    if recovered:
        await notify_safely(
            bot_token,
            chat_id,
            format_alert("List 抓取已恢复", f"{source['name']} / List {watch_list['list_id']} 已恢复正常。", f"本次返回 {item_count} 条，重复内容也视为抓取正常。"),
        )


def mark_binding_failure(db: Session, binding: WatchListBinding, watch_list: WatchList, error: str) -> None:
    binding.healthy = False
    binding.last_error = error[:2000]
    binding.last_checked_at = utc_now()
    watch_list.healthy = False
    watch_list.last_error = error[:2000]
    watch_list.last_checked_at = utc_now()
    add_log(db, "ERROR", f"List {watch_list.list_id} / RSSHub {binding.rsshub_instance_id} 抓取失败: {error}")


async def notify_safely(bot_token: str, chat_id: str, message: str) -> None:
    try:
        await send_telegram_with_retry(bot_token, chat_id, message, "报警消息")
    except Exception as exc:
        with session_scope() as db:
            add_log(db, "ERROR", f"TG 报警发送失败: {exc}")


async def send_telegram_with_retry(
    bot_token: str,
    chat_id: str,
    message: str,
    label: str,
    button_text: str = "",
    button_url: str = "",
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            await send_telegram(bot_token, chat_id, message, button_text=button_text, button_url=button_url)
            if attempt > 1:
                with session_scope() as db:
                    add_log(db, "INFO", f"{label} 第 {attempt} 次重试发送成功")
            return
        except Exception as exc:
            last_error = exc
            with session_scope() as db:
                add_log(db, "ERROR", f"{label} 第 {attempt} 次发送失败: {exc}")
    if last_error:
        raise last_error


def read_notify_settings() -> tuple[str, str, str]:
    with session_scope() as db:
        return (
            get_setting(db, "telegram_bot_token", ""),
            get_setting(db, "telegram_chat_id", ""),
            get_setting(db, "apprise_urls", ""),
        )


async def translate_via_failover(text: str, prefer_active: bool = False) -> tuple[str, str]:
    with session_scope() as db:
        enabled = get_setting(db, "translate_enabled", "0") == "1"
        if not enabled or not text.strip():
            return "", "primary"
        active_slot = get_setting(db, "translate_active_slot", "primary")
        last_primary_probe_at = parse_datetime(get_setting(db, "translate_last_primary_probe_at", ""))
        primary = {
            "api_key": get_setting(db, "translate_api_key_primary", ""),
            "model": get_setting(db, "translate_model_primary", "gpt-4.1-mini"),
            "base_url": get_setting(db, "translate_base_url_primary", "https://api.openai.com/v1"),
            "slot": "primary",
        }
        backup = {
            "api_key": get_setting(db, "translate_api_key_backup", ""),
            "model": get_setting(db, "translate_model_backup", ""),
            "base_url": get_setting(db, "translate_base_url_backup", "https://api.openai.com/v1"),
            "slot": "backup",
        }
    slots = [primary, backup]
    if prefer_active and active_slot == "backup":
        slots = [backup, primary]
        probe_due = (
            last_primary_probe_at is None
            or (utc_now() - last_primary_probe_at) >= timedelta(minutes=30)
        )
        if probe_due and primary["api_key"] and primary["model"]:
            try:
                endpoint = build_endpoint(
                    primary["api_key"],
                    primary["model"],
                    primary["base_url"],
                )
                translated = await translate_text(endpoint, text)
                with session_scope() as db:
                    set_setting(db, "translate_active_slot", "primary")
                    set_setting(db, "translate_last_primary_probe_at", utc_now().isoformat())
                    add_log(db, "INFO", "主用翻译接口恢复可用，已自动切回主用")
                return translated, "primary"
            except (OpenAIConfigError, OpenAIRequestError):
                with session_scope() as db:
                    set_setting(db, "translate_last_primary_probe_at", utc_now().isoformat())
    for slot in slots:
        if not slot["api_key"] or not slot["model"]:
            continue
        try:
            endpoint = build_endpoint(
                slot["api_key"],
                slot["model"],
                slot["base_url"],
            )
            translated = await translate_text(endpoint, text)
            with session_scope() as db:
                set_active_slot = slot["slot"]
                previous_slot = get_setting(db, "translate_active_slot", "primary")
                set_setting(db, "translate_active_slot", set_active_slot)
                if previous_slot != set_active_slot:
                    add_log(db, "INFO", f"翻译接口当前切换到{'主用' if set_active_slot == 'primary' else '备用'}")
            return translated, slot["slot"]
        except (OpenAIConfigError, OpenAIRequestError):
            continue
    return "", active_slot


async def maybe_translate_title(text: str) -> str:
    translated, _ = await translate_via_failover(text)
    return translated


def is_retweet_text(value: str) -> bool:
    return value.strip().lower().startswith(("rt ", "rt\u2002", "转发 "))


def extract_retweet_source(value: str) -> tuple[str, str]:
    text = value.strip()
    match = re.match(r"(?is)^RT[\s\u2002]+@?([A-Za-z0-9_]{1,20})\s*\n+(.*)$", text)
    if not match:
        match = re.match(r"(?is)^RT[\s\u2002]+@?([A-Za-z0-9_]{1,20})\s+(.*)$", text)
    if not match:
        return "", value
    return match.group(1), match.group(2).strip()


def extract_quote_source(value: str) -> tuple[str, str]:
    text = value.strip()
    if not text:
        return "", ""
    match = re.match(r"(?is)^([^:\n：]{1,60})[:：][\s\u2002]*(.*)$", text)
    if not match:
        return "", value
    source = re.sub(r"\s+", " ", match.group(1)).strip().lstrip("@")
    return source, match.group(2).strip()


def read_int_setting(key: str, default: int) -> int:
    with session_scope() as db:
        value = get_setting(db, key, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def snapshot_rsshub(rsshub: RsshubInstance, binding_id: int) -> dict:
    return {
        "id": rsshub.id,
        "binding_id": binding_id,
        "name": rsshub.name,
        "rsshub_url": rsshub.internal_url,
    }


def snapshot_list(watch_list: WatchList) -> dict:
    return {
        "id": watch_list.id,
        "name": watch_list.name,
        "list_id": watch_list.list_id,
        "rsshub_instance_id": watch_list.rsshub_instance_id,
    }


def resolve_list_rsshub(db: Session, watch_list: WatchList) -> RsshubInstance | None:
    if watch_list.rsshub_instance_id:
        rsshub = (
            db.query(RsshubInstance)
            .filter(RsshubInstance.id == watch_list.rsshub_instance_id)
            .first()
        )
        if rsshub:
            return rsshub
    rsshub = db.query(RsshubInstance).order_by(RsshubInstance.host_port.asc(), RsshubInstance.id.asc()).first()
    if rsshub and watch_list.rsshub_instance_id != rsshub.id:
        watch_list.rsshub_instance_id = rsshub.id
    return rsshub


def resolve_author_label(username: str) -> str:
    if not username:
        return ""
    with session_scope() as db:
        alias = db.query(UserAlias).filter(UserAlias.username == username).first()
        if alias:
            return f"{alias.note}----@{username}"
    return f"@{username}"


def normalize_username(value: str) -> str:
    value = value.strip().removeprefix("@").lower()
    return value


def extract_username_from_entry(entry) -> str:
    for key in ("author", "authors"):
        value = entry.get(key)
        if isinstance(value, str):
            username = extract_username_from_text(value)
            if username:
                return username
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    username = extract_username_from_text(str(item.get("name") or item.get("href") or ""))
                    if username:
                        return username
    return extract_username_from_item(
        {
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "id": entry.get("id") or entry.get("guid") or "",
        }
    )


def extract_username_from_item(item: dict) -> str:
    for value in (str(item.get("link") or ""), str(item.get("id") or "")):
        match = re.search(r"(?:x|twitter)\.com/([^/?#]+)/status/\d+", value)
        if match and match.group(1).lower() not in {"i", "intent"}:
            return normalize_username(match.group(1))
    return extract_username_from_text(str(item.get("title") or ""))


def extract_username_from_text(value: str) -> str:
    match = re.search(r"@([A-Za-z0-9_]{1,20})", value)
    return normalize_username(match.group(1)) if match else ""


def stable_id(link: str, title: str) -> str:
    digest = hashlib.sha256(f"{link}\n{title}".encode("utf-8")).hexdigest()
    return f"feed:{digest}"


def normalize_item_id(item: dict) -> str:
    raw_id = str(item.get("id") or "")
    link = str(item.get("link") or "")
    title = str(item.get("title") or "")
    for value in (raw_id, link):
        tweet_id = extract_tweet_id(value)
        if tweet_id:
            return f"tweet:{tweet_id}"
    if raw_id:
        return raw_id if raw_id.startswith("tweet:") else f"item:{raw_id}"
    return stable_id(link, title)


def candidate_item_ids(item: dict) -> list[str]:
    raw_id = str(item.get("id") or "")
    link = str(item.get("link") or "")
    title = str(item.get("title") or "")
    ids = [normalize_item_id(item)]
    if raw_id:
        ids.append(raw_id)
    if link:
        ids.append(link)
    ids.append(stable_id(link, title))
    return list(dict.fromkeys(ids))


def extract_tweet_id(value: str) -> str:
    patterns = [
        r"(?:twitter\.com|x\.com)/[^/\s]+/status/(\d+)",
        r"/i/web/status/(\d+)",
        r"(?:^|[:/_-])status[:/_-]?(\d{5,})",
        r"^(\d{5,})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(1)
    return ""


def elapsed_since(then: datetime, now: datetime) -> timedelta:
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now - then


watcher = Watcher()
