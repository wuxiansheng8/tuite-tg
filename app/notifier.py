from __future__ import annotations

import html
import re
from typing import Optional

import apprise
import httpx


class NotifyError(RuntimeError):
    pass


def html_to_text(value: str) -> str:
    if not value:
        return ""
    text = html.unescape(value)
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?i)</\s*(p|div|li|tr)\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*(p|div|li|tr|hr)\b[^>]*>", "\n", text)
    text = re.sub(r"(?i)<\s*/?\s*video\b[^>]*>", "\n", text)
    text = re.sub(r"(?i)<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_rsshub_description(value: str) -> tuple[str, str]:
    if not value:
        return "", ""
    text = html.unescape(value)
    quote_pattern = re.compile(
        r'(?is)(?:<hr[^>]*>\s*)?<div[^>]*class=["\'][^"\']*rsshub-quote[^"\']*["\'][^>]*>(.*?)</div>\s*'
    )
    match = quote_pattern.search(text)
    if not match:
        return html_to_text(text), ""
    quote_html = match.group(1)
    outer_html = quote_pattern.sub("\n", text, count=1)
    return html_to_text(outer_html), html_to_text(quote_html)


def clip_text(value: str, limit: int = 2800) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


async def send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    button_text: str = "",
    button_url: str = "",
) -> None:
    if not bot_token or not chat_id:
        raise NotifyError("Telegram bot token 或 chat id 未配置")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if button_text and button_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": button_text, "url": button_url}]]
        }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            url,
            json=payload,
        )
    if resp.status_code >= 400:
        raise NotifyError(f"Telegram 推送失败: {resp.status_code} {resp.text[:300]}")


def send_apprise(urls: str, title: str, body: str) -> bool:
    targets = [line.strip() for line in urls.replace(",", "\n").splitlines() if line.strip()]
    if not targets:
        return True
    app = apprise.Apprise()
    for target in targets:
        app.add(target)
    return bool(app.notify(title=title, body=body))


def format_alert(title: str, body: str, detail: Optional[str] = None) -> str:
    message = f"<b>{html.escape(title)}</b>\n{html.escape(body)}"
    if detail:
        message += f"\n\n<code>{html.escape(detail[:1200])}</code>"
    return message


def format_feed_item(
    author_label: str = "",
    translated_outer: str = "",
    translated_quote: str = "",
    is_retweet: bool = False,
    retweet_source: str = "",
    quote_source: str = "",
) -> str:
    parts = []
    if author_label:
        parts.append(html.escape(author_label))
    if translated_outer:
        if parts:
            parts.append("")
        if is_retweet:
            heading = f"转发自@{retweet_source}" if retweet_source else "转发"
            parts.append(f"<b>{html.escape(heading)}</b>\n{html.escape(clip_text(translated_outer))}")
        else:
            parts.append(html.escape(clip_text(translated_outer)))
    if translated_quote:
        if parts:
            parts.append("")
        heading = f"引用自@{quote_source}" if quote_source else "引用"
        parts.append(f"<b>{html.escape(heading)}</b>\n{html.escape(clip_text(translated_quote))}")
    body = "\n".join(parts)
    return body
