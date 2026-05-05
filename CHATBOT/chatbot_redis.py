from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone

MYT = timezone(timedelta(hours=8))

CHATBOT_LOG_KEY_PREFIX = "chatbot:logs:"
CHATBOT_LAST_SYNCED_KEY = "chatbot:last_synced_date"
CHATBOT_LOG_TTL = 30 * 86400  # 30 days


def get_redis_client():
    try:
        from upstash_redis import Redis
    except ImportError:
        return None
    url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
    if not url or not token:
        try:
            import streamlit as st
            if not url:
                url = (st.secrets.get("UPSTASH_REDIS_REST_URL") or "").strip()
            if not token:
                token = (st.secrets.get("UPSTASH_REDIS_REST_TOKEN") or "").strip()
        except Exception:
            pass
    if not url or not token:
        return None
    try:
        from upstash_redis import Redis
        return Redis(url=url, token=token)
    except Exception:
        return None


def get_chatbot_redis_client():
    """Returns a Redis client for the dedicated chatbot logs database (UPSTASH_CHATBOT_REST_URL/TOKEN)."""
    try:
        from upstash_redis import Redis
    except ImportError:
        return None
    url = os.getenv("UPSTASH_CHATBOT_REST_URL", "").strip()
    token = os.getenv("UPSTASH_CHATBOT_REST_TOKEN", "").strip()
    if not url or not token:
        try:
            import streamlit as st
            if not url:
                url = (st.secrets.get("UPSTASH_CHATBOT_REST_URL") or "").strip()
            if not token:
                token = (st.secrets.get("UPSTASH_CHATBOT_REST_TOKEN") or "").strip()
        except Exception:
            pass
    if not url or not token:
        return None
    try:
        return Redis(url=url, token=token)
    except Exception:
        return None


def log_qa_to_redis(r, user_name: str, question: str, answer: str, tokens_used: int,
                    email: str = "", cell: str = "") -> None:
    now = datetime.now(MYT)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    payload = json.dumps({
        "email": email,
        "user_name": user_name,
        "cell": cell,
        "question": question,
        "answer": answer,
        "timestamp": time_str,
        "tokens_used": tokens_used,
    })
    key = f"{CHATBOT_LOG_KEY_PREFIX}{date_str}"
    r.rpush(key, payload)
    r.expire(key, CHATBOT_LOG_TTL)


def get_unsynced_logs(r, today_myt_str: str) -> list[dict]:
    raw_last = r.get(CHATBOT_LAST_SYNCED_KEY)
    if isinstance(raw_last, bytes):
        raw_last = raw_last.decode()
    raw_last = (raw_last or "").strip()

    today = date.fromisoformat(today_myt_str)
    yesterday = today - timedelta(days=1)

    if raw_last:
        start = date.fromisoformat(raw_last) + timedelta(days=1)
    else:
        start = today - timedelta(days=30)

    if start > yesterday:
        return []

    logs = []
    current = start
    while current <= yesterday:
        date_str = current.isoformat()
        raw_items = r.lrange(f"{CHATBOT_LOG_KEY_PREFIX}{date_str}", 0, -1) or []
        for raw in raw_items:
            s = raw.decode() if isinstance(raw, bytes) else raw
            entry = json.loads(s)
            entry["date"] = date_str
            logs.append(entry)
        current += timedelta(days=1)
    return logs


def mark_synced(r, date_str: str) -> None:
    r.set(CHATBOT_LAST_SYNCED_KEY, date_str)


# ── Change requests ────────────────────────────────────────────────────────────

CHANGE_REQ_KEY_PREFIX = "change_requests:"
CHANGE_REQ_LAST_SYNCED_KEY = "change_requests:last_synced_date"
CHANGE_REQ_TTL = 30 * 86400  # 30 days


def submit_change_request(r, data: dict) -> None:
    now = datetime.now(MYT)
    date_str = now.strftime("%Y-%m-%d")
    payload = json.dumps({
        **data,
        "date": date_str,
        "timestamp": now.strftime("%H:%M:%S"),
        "status": "Pending",
    })
    key = f"{CHANGE_REQ_KEY_PREFIX}{date_str}"
    r.rpush(key, payload)
    r.expire(key, CHANGE_REQ_TTL)


def get_unsynced_change_requests(r, today_myt_str: str) -> list[dict]:
    raw_last = r.get(CHANGE_REQ_LAST_SYNCED_KEY)
    if isinstance(raw_last, bytes):
        raw_last = raw_last.decode()
    raw_last = (raw_last or "").strip()

    today = date.fromisoformat(today_myt_str)
    yesterday = today - timedelta(days=1)

    if raw_last:
        start = date.fromisoformat(raw_last) + timedelta(days=1)
    else:
        start = today - timedelta(days=30)

    if start > yesterday:
        return []

    items = []
    current = start
    while current <= yesterday:
        date_str = current.isoformat()
        raw_items = r.lrange(f"{CHANGE_REQ_KEY_PREFIX}{date_str}", 0, -1) or []
        for raw in raw_items:
            s = raw.decode() if isinstance(raw, bytes) else raw
            entry = json.loads(s)
            entry["date"] = date_str
            items.append(entry)
        current += timedelta(days=1)
    return items


def mark_change_requests_synced(r, date_str: str) -> None:
    r.set(CHANGE_REQ_LAST_SYNCED_KEY, date_str)
