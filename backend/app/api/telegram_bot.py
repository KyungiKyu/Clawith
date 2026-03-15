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
        logger.error(f"[Telegram] Failed to register webhook: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to register webhook with Telegram: {str(e)}")

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
        return ChannelConfigOut.model_validate(existing)

    config = ChannelConfig(
        agent_id=agent_id,
        channel_type="telegram",
        app_secret=bot_token,
        is_configured=True,
    )
    db.add(config)
    await db.flush()
    return ChannelConfigOut.model_validate(config)


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

    bot_token = config.app_secret
    if bot_token:
        tg_api_url = f"https://api.telegram.org/bot{bot_token}/deleteWebhook"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(tg_api_url)
        except Exception as e:
            logger.warning(f"[Telegram] Failed to delete webhook on removal: {e}")

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


async def _process_telegram_message(agent_id: uuid.UUID, sender_id: int, display_name: str, conv_id: str, user_text: str, chat_id: int):
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

        # Load history
        history_r = await bg_db.execute(
            select(ChatMessage)
            .where(ChatMessage.agent_id == agent_id, ChatMessage.conversation_id == session_conv_id)
            .order_by(ChatMessage.created_at.desc())
            .limit(ctx_size)
        )
        history = [{"role": m.role, "content": m.content} for m in reversed(history_r.scalars().all())]

        # Save user message
        bg_db.add(ChatMessage(agent_id=agent_id, user_id=platform_user_id, role="user", content=user_text, conversation_id=session_conv_id))
        sess.last_message_at = datetime.now(timezone.utc)
        await bg_db.commit()

        # Mark thinking (Typing action in Telegram)
        tg_action_url = f"https://api.telegram.org/bot{bot_token}/sendChatAction"
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(tg_action_url, json={"chat_id": chat_id, "action": "typing"})
        except Exception:
            pass

        # Call LLM
        reply_text = await _call_agent_llm(bg_db, agent_id, user_text, history=history)

        # Save reply
        bg_db.add(ChatMessage(agent_id=agent_id, user_id=platform_user_id, role="assistant", content=reply_text, conversation_id=session_conv_id))
        sess.last_message_at = datetime.now(timezone.utc)
        await bg_db.commit()

        # Send reply
        tg_send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Telegram limits messages to 4096 chars, chunk if necessary
                limit = 4050
                chunks = [reply_text[i:i+limit] for i in range(0, len(reply_text), limit)]
                for chunk in chunks:
                    await client.post(tg_send_url, json={"chat_id": chat_id, "text": chunk})
        except Exception as e:
            logger.error(f"[Telegram] Failed to send reply: {e}")
