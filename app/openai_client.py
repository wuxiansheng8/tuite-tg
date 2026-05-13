from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx


class OpenAIConfigError(RuntimeError):
    pass


class OpenAIRequestError(RuntimeError):
    pass


@dataclass
class OpenAIEndpoint:
    api_key: str
    model: str
    base_url: str


def build_endpoint(
    api_key: str,
    model: str,
    base_url: str,
) -> OpenAIEndpoint:
    clean_key = api_key.strip()
    clean_model = model.strip()
    clean_base = (base_url or "https://api.openai.com/v1").strip().rstrip("/")
    if not clean_key:
        raise OpenAIConfigError("OpenAI API Key 未填写")
    if not clean_model:
        raise OpenAIConfigError("OpenAI 模型未填写")
    return OpenAIEndpoint(
        api_key=clean_key,
        model=clean_model,
        base_url=clean_base,
    )


def _headers(endpoint: OpenAIEndpoint) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {endpoint.api_key}",
        "Content-Type": "application/json",
    }


async def translate_text(endpoint: OpenAIEndpoint, text: str) -> str:
    system_prompt = "你是一个翻译助手。把用户提供的推文内容翻译成简体中文。只输出翻译结果，不要解释，不要加引号。如果原文已经是中文，就直接返回原文。"
    payload = {
        "model": endpoint.model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text.strip()},
        ],
        "temperature": 0.2,
    }
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.post(
            f"{endpoint.base_url}/chat/completions",
            headers=_headers(endpoint),
            json=payload,
        )
    if resp.status_code >= 400:
        raise OpenAIRequestError(f"翻译请求失败: {resp.status_code} {resp.text[:300]}")
    body = resp.json()
    output_text = str(
        (((body.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
    ).strip()
    if output_text:
        return output_text
    raise OpenAIRequestError("翻译请求成功，但没有返回文本结果")


async def query_recent_costs(endpoint: OpenAIEndpoint, days: int = 30) -> str:
    start_time = int((datetime.now(timezone.utc) - timedelta(days=max(1, days))).timestamp())
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.get(
            f"{endpoint.base_url}/organization/usage/costs",
            headers=_headers(endpoint),
            params={"start_time": start_time},
        )
    if resp.status_code >= 400:
        async with httpx.AsyncClient(timeout=45.0) as client:
            fallback_resp = await client.get(
                f"{endpoint.base_url}/user/balance",
                headers=_headers(endpoint),
            )
        if fallback_resp.status_code >= 400:
            raise OpenAIRequestError(f"余额查询失败: {resp.status_code} {resp.text[:180]} | 备用接口: {fallback_resp.status_code} {fallback_resp.text[:180]}")
        balance_body = fallback_resp.json()
        if "balance_infos" in balance_body:
            infos = balance_body.get("balance_infos") or []
            total = sum(float(item.get("total_balance") or 0) for item in infos)
            currency = str((infos[0].get("currency") if infos else "CNY") or "CNY").upper()
            return f"当前余额：{total:.4f} {currency}"
        if "balance" in balance_body:
            return f"当前余额：{balance_body['balance']}"
        raise OpenAIRequestError(f"余额接口返回了未知格式: {fallback_resp.text[:200]}")
    body = resp.json()
    total = 0.0
    currency = "USD"
    for row in body.get("data", []):
        amount = row.get("amount") or {}
        value = amount.get("value")
        if value is None:
            continue
        total += float(value)
        currency = str(amount.get("currency") or currency).upper()
    return f"最近 {days} 天费用：{total:.4f} {currency}"
