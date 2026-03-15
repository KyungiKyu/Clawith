"""Telegram Bot Channel API routes."""

import logging
import uuid
import httpx

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.permissions import check_agent_access, is_agent_creator
from app.core.security import get_current_user
from app.database import get_db, async_session
from app.models.channel_config import ChannelConfig
from app.models.user import User
from app.schemas.schemas import ChannelConfigOut

logger = logging.getLogger(__name__)

router = APIRouter(tags=["telegram"])


# ─── Config CRUD ────────────────────────────────────────

@router.post("/agents/{agent_id}/telegram-channel", response_model=ChannelConfigOut, status_code=201)
async def configure_telegram_channel(
    agent_id: uuid.UUID,
    data: dict,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Configure Telegram bot for an agent. Fields: bot_token."""
    agent, _ = await check_agent_access(db, current_user, agent_id)
    if not is_agent_creator(current_user, agent):
        raise HTTPException(status_code=403, detail="Only creator can configure channel")

    bot_token = data.get("bot_token", "").strip()
    if not bot_token:
        raise HTTPException(status_code=422, detail="bot_token is required")

    # Determine public base URL for webhook registration
    import os
    from app.models.system_settings import SystemSetting
    public_base = ""
    result = await db.execute(select(SystemSetting).where(SystemSetting.key == "platform"))
    setting = result.scalar_one_or_none()
    if setting and setting.value.get("public_base_url"):
        public_base = setting.value["public_base_url"].rstrip("/")
    if not public_base:
        public_base = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    if not public_base:
        public_base = str(request.base_url).rstrip("/")
        
    webhook_url = f"{public_base}/api/channel/telegram/{agent_id}/webhook"

    # Register Webhook with Telegram
    tg_api_url = f"https://api.telegram.org/bot{bot_token}/setWebhook"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(tg_api_url, json={"url": webhook_url})
            resp.raise_for_status()
            logger.info(f"[Telegram] Webhook registered for agent {agent_id}: {resp.json()}")
    except Exception as e:
        logger.error(f"[Telegram] Warning: Failed to register webhook: {e}")
        # We do not raise HTTPException here to allow saving the bot token 
        # even if the environment doesn't have a valid HTTPS public URL yet.

    result = await db.execute(
        select(ChannelConfig).where(
            ChannelConfig.agent_id == agent_id,
            ChannelConfig.channel_type == "telegram",
        )
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.app_secret = bot_token
        existing.is_configured = True
        await db.flush()
        out = ChannelConfigOut.model_validate(existing)
        # (Re)start long-polling for this agent so messages work immediately
        from app.services.telegram_poller import start_polling_for_agent
        start_polling_for_agent(agent_id, bot_token)
        return out

    config = ChannelConfig(
        agent_id=agent_id,
        channel_type="telegram",
        app_secret=bot_token,
        is_configured=True,
    )
    db.add(config)
    await db.flush()
    out = ChannelConfigOut.model_validate(config)
    from app.services.telegram_poller import start_polling_for_agent
    start_polling_for_agent(agent_id, bot_token)
    return out


@router.get("/agents/{agent_id}/telegram-channel", response_model=ChannelConfigOut)
async def get_telegram_channel(
    agent_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await check_agent_access(db, current_user, agent_id)
    result = await db.execute(
        select(ChannelConfig).where(
            ChannelConfig.agent_id == agent_id,
            ChannelConfig.channel_type == "telegram",
        )
    )
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Telegram not configured")
    return ChannelConfigOut.model_validate(config)


@router.delete("/agents/{agent_id}/telegram-channel", status_code=204)
async def delete_telegram_channel(
    agent_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    agent, _ = await check_agent_access(db, current_user, agent_id)
    if not is_agent_creator(current_user, agent):
        raise HTTPException(status_code=403, detail="Only creator can remove channel")
    result = await db.execute(
        select(ChannelConfig).where(
            ChannelConfig.agent_id == agent_id,
            ChannelConfig.channel_type == "telegram",
        )
    )
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Telegram not configured")

    # Stop the long-poller for this agent
    from app.services.telegram_poller import stop_polling_for_agent
    stop_polling_for_agent(agent_id)

    await db.delete(config)


# ─── Webhook Receiver ───────────────────────────────────

@router.post("/channel/telegram/{agent_id}/webhook")
async def telegram_webhook(
    agent_id: uuid.UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Handle incoming Telegram messages."""
    try:
        data = await request.json()
    except Exception:
        return Response(status_code=400)

    # We only care about normal messages for now
    if "message" not in data:
        return Response(status_code=200)

    msg_obj = data["message"]
    text = msg_obj.get("text", "").strip()
    if not text:
        return Response(status_code=200)

    chat_id = msg_obj.get("chat", {}).get("id")
    sender_id = msg_obj.get("from", {}).get("id")
    if not chat_id or not sender_id:
        return Response(status_code=200)

    sender_username = msg_obj.get("from", {}).get("username")
    sender_first = msg_obj.get("from", {}).get("first_name", "")
    sender_last = msg_obj.get("from", {}).get("last_name", "")
    display_name = sender_username
    if not display_name:
        display_name = f"{sender_first} {sender_last}".strip() or f"TG User {sender_id}"

    conv_id = f"telegram_{chat_id}"
    
    # Fire and forget backend task
    import asyncio
    asyncio.create_task(_process_telegram_message(agent_id, sender_id, display_name, conv_id, text, chat_id))

    # Telegram requires an immediate 200 OK
    return Response(status_code=200)


async def _process_telegram_message(
    agent_id: uuid.UUID, sender_id: int, display_name: str,
    conv_id: str, user_text: str, chat_id: int,
    image_path: str | None = None,
):
    """Background task to call LLM and send reply to Telegram."""
    from app.models.audit import ChatMessage
    from app.models.agent import Agent as AgentModel
    from app.api.feishu import _call_agent_llm
    from app.services.channel_session import find_or_create_channel_session
    from datetime import datetime, timezone

    async with async_session() as bg_db:
        # Load agent config and token
        agent_r = await bg_db.execute(select(AgentModel).where(AgentModel.id == agent_id))
        agent_obj = agent_r.scalar_one_or_none()
        if not agent_obj:
            return
            
        cfg_r = await bg_db.execute(select(ChannelConfig).where(
            ChannelConfig.agent_id == agent_id,
            ChannelConfig.channel_type == "telegram",
        ))
        cfg = cfg_r.scalar_one_or_none()
        if not cfg or not cfg.app_secret:
            return
            
        bot_token = cfg.app_secret
        ctx_size = agent_obj.context_window_size or 20

        # Find-or-create user
        from app.models.user import User as _User
        from app.core.security import hash_password as _hp
        import uuid as _uuid
        
        _username = f"telegram_{sender_id}"
        _u_r = await bg_db.execute(select(_User).where(_User.username == _username))
        _platform_user = _u_r.scalar_one_or_none()
        if not _platform_user:
            _platform_user = _User(
                username=_username,
                email=f"{_username}@telegram.local",
                password_hash=_hp(_uuid.uuid4().hex),
                display_name=display_name,
                role="member",
                tenant_id=agent_obj.tenant_id,
            )
            bg_db.add(_platform_user)
            await bg_db.flush()
        platform_user_id = _platform_user.id

        # Find-or-create ChatSession
        sess = await find_or_create_channel_session(
            db=bg_db,
            agent_id=agent_id,
            user_id=platform_user_id,
            external_conv_id=conv_id,
            source_channel="telegram",
            first_message_title=user_text[:50],
        )
        session_conv_id = str(sess.id)

        # Load history — only user/assistant messages; skip tool_call/tool records
        # that the websocket saves, as those roles are not valid in the channel LLM path.
        history_r = await bg_db.execute(
            select(ChatMessage)
            .where(
                ChatMessage.agent_id == agent_id,
                ChatMessage.conversation_id == session_conv_id,
                ChatMessage.role.in_(["user", "assistant"]),
            )
            .order_by(ChatMessage.created_at.desc())
            .limit(ctx_size)
        )
        history = [{"role": m.role, "content": m.content} for m in reversed(history_r.scalars().all())]

        # Save user message and commit BEFORE calling LLM.
        # _call_agent_llm internally opens its own async_session() calls,
        # so we must not hold an open transaction at the same time.
        bg_db.add(ChatMessage(agent_id=agent_id, user_id=platform_user_id, role="user", content=user_text, conversation_id=session_conv_id))
        sess.last_message_at = datetime.now(timezone.utc)
        await bg_db.commit()

        # Build LLM prompt — inject minimal Telegram channel context.
        # When there is an image, user_text already contains [image_data:base64...]
        # from the poller. Do NOT add a file-path tool hint — that confuses the
        # model into calling read_document instead of using its vision capability.
        channel_ctx = (
            f"[System: Responding via Telegram to '{display_name}'. "
            f"Use Telegram Markdown (*bold*, _italic_, `code`) and keep replies concise.]"
        )
        llm_text = channel_ctx + "\n" + user_text

        # Mark thinking (Typing action in Telegram)
        tg_action_url = f"https://api.telegram.org/bot{bot_token}/sendChatAction"
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(tg_action_url, json={"chat_id": chat_id, "action": "typing"})
        except Exception:
            pass

        # Call LLM — use a fresh session to avoid conflicts with call_llm's internal sessions
        from app.database import async_session as _async_session
        logger.info(f"[Telegram] Calling LLM for agent {agent_id}, has_image={image_path is not None}, text_len={len(llm_text)}")
        async with _async_session() as llm_db:
            reply_text = await _call_agent_llm(llm_db, agent_id, llm_text, history=history, user_id=platform_user_id)
        logger.info(f"[Telegram] LLM reply ({len(reply_text)} chars): {reply_text[:100]!r}")
        if not reply_text or reply_text == "[LLM returned empty content]":
            # Retry once without history in case history was the problem
            logger.warning(f"[Telegram] Empty reply, retrying without history...")
            async with _async_session() as llm_db2:
                reply_text = await _call_agent_llm(llm_db2, agent_id, user_text, history=[], user_id=platform_user_id)
            logger.info(f"[Telegram] Retry reply ({len(reply_text)} chars): {reply_text[:100]!r}")

        # Save reply
        async with _async_session() as save_db:
            save_db.add(ChatMessage(agent_id=agent_id, user_id=platform_user_id, role="assistant", content=reply_text, conversation_id=session_conv_id))
            await save_db.commit()

        # Send reply
        tg_send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Telegram limits messages to 4096 chars, chunk if necessary
                limit = 4050
                _text_str = str(reply_text)
                chunks = [_text_str[i:i+limit] for i in range(0, len(_text_str), limit)]
                for chunk in chunks:
                    payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"}
                    resp = await client.post(tg_send_url, json=payload)
                    if resp.status_code != 200:
                        # Markdown parse error (e.g. unbalanced `` or *) — retry as plain text
                        await client.post(tg_send_url, json={"chat_id": chat_id, "text": chunk})
        except Exception as e:
            logger.error(f"[Telegram] Failed to send reply: {e}")
