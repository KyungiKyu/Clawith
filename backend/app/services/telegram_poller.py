"""Telegram long-polling background service.

Instead of relying on Telegram webhooks (which require HTTPS), this module
polls the Telegram Bot API using getUpdates. One asyncio task runs per
configured Telegram bot. Tasks are started/stopped dynamically as agents
configure or remove their Telegram bot tokens.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Dict

import httpx

logger = logging.getLogger(__name__)

# Maps agent_id (str) -> asyncio.Task
_polling_tasks: Dict[str, asyncio.Task] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def start_polling_for_agent(agent_id: uuid.UUID, bot_token: str) -> None:
    """Start (or restart) long-polling for a single agent's Telegram bot."""
    key = str(agent_id)
    _cancel_if_running(key)
    task = asyncio.create_task(
        _poll_loop(agent_id, bot_token),
        name=f"telegram-poll-{key}",
    )
    _polling_tasks[key] = task
    logger.info(f"[TelegramPoller] Started polling for agent {key}")


def stop_polling_for_agent(agent_id: uuid.UUID) -> None:
    """Stop long-polling for a single agent (e.g. on disconnect)."""
    _cancel_if_running(str(agent_id))


async def start_all_pollers() -> None:
    """Called at app startup – start pollers for every configured Telegram bot."""
    from sqlalchemy import select
    from app.database import async_session
    from app.models.channel_config import ChannelConfig

    async with async_session() as db:
        result = await db.execute(
            select(ChannelConfig).where(
                ChannelConfig.channel_type == "telegram",
                ChannelConfig.is_configured == True,  # noqa: E712
            )
        )
        configs = result.scalars().all()

    for cfg in configs:
        if cfg.app_secret:
            start_polling_for_agent(cfg.agent_id, cfg.app_secret)

    logger.info(f"[TelegramPoller] Launched {len(configs)} poller(s) at startup")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cancel_if_running(key: str) -> None:
    existing = _polling_tasks.get(key)
    if existing and not existing.done():
        existing.cancel()
    _polling_tasks.pop(key, None)


async def _poll_loop(agent_id: uuid.UUID, bot_token: str) -> None:
    """Main polling loop for one bot. Runs indefinitely until cancelled."""
    base_url = f"https://api.telegram.org/bot{bot_token}"
    offset: int | None = None
    backoff = 1

    # First, delete any existing webhook so getUpdates works
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{base_url}/deleteWebhook", json={"drop_pending_updates": False})
            logger.info(f"[TelegramPoller] Cleared webhook for agent {agent_id}")
    except Exception as e:
        logger.warning(f"[TelegramPoller] Could not clear webhook for agent {agent_id}: {e}")

    while True:
        try:
            params: dict = {"timeout": 30, "limit": 50, "allowed_updates": ["message"]}
            if offset is not None:
                params["offset"] = offset

            async with httpx.AsyncClient(timeout=45) as client:
                resp = await client.get(f"{base_url}/getUpdates", params=params)

            if resp.status_code == 401:
                logger.error(f"[TelegramPoller] Invalid token for agent {agent_id}, stopping.")
                return

            if resp.status_code != 200:
                logger.warning(f"[TelegramPoller] Non-200 from getUpdates: {resp.status_code}, retrying")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            backoff = 1
            data = resp.json()
            updates = data.get("result", [])

            for update in updates:
                offset = update["update_id"] + 1
                asyncio.create_task(_handle_update(agent_id, bot_token, update))

        except asyncio.CancelledError:
            logger.info(f"[TelegramPoller] Polling cancelled for agent {agent_id}")
            return
        except Exception as e:
            logger.warning(f"[TelegramPoller] Error in poll loop for agent {agent_id}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def _handle_update(agent_id: uuid.UUID, bot_token: str, update: dict) -> None:
    """Process a single Telegram update (text or photo)."""
    msg_obj = update.get("message")
    if not msg_obj:
        return

    chat_id = msg_obj.get("chat", {}).get("id")
    sender_id = msg_obj.get("from", {}).get("id")
    if not chat_id or not sender_id:
        return

    sender_username = msg_obj.get("from", {}).get("username")
    sender_first = msg_obj.get("from", {}).get("first_name", "")
    sender_last = msg_obj.get("from", {}).get("last_name", "")
    display_name = sender_username or f"{sender_first} {sender_last}".strip() or f"TG User {sender_id}"
    conv_id = f"telegram_{chat_id}"

    text = msg_obj.get("text", "").strip()
    caption = msg_obj.get("caption", "").strip()  # caption on photos/files
    photos = msg_obj.get("photo")  # list of PhotoSize objects, largest last
    image_path: str | None = None

    # ── Photo handling ────────────────────────────────────────────────────────
    if photos:
        # Pick the highest-resolution version (Telegram sends multiple sizes)
        best = max(photos, key=lambda p: p.get("file_size", 0))
        file_id = best.get("file_id")
        img_bytes: bytes | None = None
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                # Step 1: resolve file_id → download path
                finfo_resp = await client.get(
                    f"https://api.telegram.org/bot{bot_token}/getFile",
                    params={"file_id": file_id},
                )
                finfo = finfo_resp.json()
                file_path_tg = finfo.get("result", {}).get("file_path")
                if file_path_tg:
                    img_resp = await client.get(
                        f"https://api.telegram.org/file/bot{bot_token}/{file_path_tg}"
                    )
                    if img_resp.status_code == 200:
                        img_bytes = img_resp.content
                        from pathlib import Path
                        from app.config import get_settings
                        settings = get_settings()
                        upload_dir = (
                            Path(settings.AGENT_DATA_DIR) / str(agent_id) / "workspace" / "uploads"
                        )
                        upload_dir.mkdir(parents=True, exist_ok=True)
                        ext = file_path_tg.rsplit(".", 1)[-1] if "." in file_path_tg else "jpg"
                        fname = f"tg_{file_id[-8:]}.{ext}"
                        save_path = upload_dir / fname
                        save_path.write_bytes(img_bytes)
                        image_path = f"workspace/uploads/{fname}"
                        logger.info(f"[TelegramPoller] Saved photo to {save_path} ({len(img_bytes)} bytes)")
        except Exception as e:
            logger.warning(f"[TelegramPoller] Failed to download photo: {e}")

        # Embed image as base64 marker so vision-capable LLMs can see it.
        # call_llm in websocket.py automatically converts [image_data:...] to
        # the OpenAI Vision API format when supports_vision=True.
        import base64 as _b64
        if img_bytes:
            b64 = _b64.b64encode(img_bytes).decode("ascii")
            image_marker = f"[image_data:data:image/jpeg;base64,{b64}]"
            user_text = (caption + "\n" if caption else "") + image_marker
        else:
            user_text = caption or "[Image received but could not be downloaded]"
    else:
        user_text = text

    if not user_text and not image_path:
        return

    # Reuse the existing processing logic from telegram_bot.py
    from app.api.telegram_bot import _process_telegram_message
    await _process_telegram_message(
        agent_id, sender_id, display_name, conv_id, user_text, chat_id,
        image_path=image_path,
    )

