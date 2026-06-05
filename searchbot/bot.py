"""aiogram v3 search-and-serve bot.

Flow:
  /start                     → register user, greet.
  /start get_<message_id>    → deep-link serve (used by inline results).
  any text                   → search catalog, show result buttons.
  callback get:<message_id>  → force-sub check, then copy_message the file.
  inline @bot <query>        → results that deep-link back to the bot to serve.
  /stats                     → admin-only catalog/user counts.
  /broadcast <text>          → admin-only fan-out to all known users.

Files are delivered with ``bot.copy_message(from_chat_id=storage, message_id=…)``
so nothing is re-uploaded. The bot MUST be an admin in the storage channel.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Message,
)

from .db import Catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("searchbot")

BASE_DIR = Path(__file__).resolve().parent.parent

dp = Dispatcher()


def load_bot_config() -> dict:
    # SEARCHBOT_CONFIG lets the container mount config from a data volume
    # (e.g. /data/config.json) instead of baking it into the image.
    path = Path(os.environ.get("SEARCHBOT_CONFIG", str(BASE_DIR / "searchbot" / "config.json")))
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — copy searchbot/config.example.json to it and fill it in."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


CFG = load_bot_config()
catalog = Catalog(CFG.get("db_path", str(BASE_DIR / "searchbot" / "catalog.db")))
STORAGE = CFG["storage_channel_id"]
FORCE_SUB = CFG.get("force_sub_channel")  # @username or -100… id, or null
ADMINS = set(CFG.get("admins", []))
RESULTS_LIMIT = int(CFG.get("results_limit", 10))
AUTO_DELETE = int(CFG.get("auto_delete_seconds", 0))

BOT_USERNAME = ""  # filled at startup


def _label(row) -> str:
    bits = [row["title"]]
    if row["year"]:
        bits.append(f"({row['year']})")
    if row["quality"]:
        bits.append(f"[{row['quality']}]")
    return " ".join(bits)


def _results_keyboard(rows) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=_label(r), callback_data=f"get:{r['message_id']}")]
        for r in rows
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _is_subscribed(bot: Bot, user_id: int) -> bool:
    if not FORCE_SUB:
        return True
    try:
        member = await bot.get_chat_member(FORCE_SUB, user_id)
        return member.status in {"member", "administrator", "creator"}
    except TelegramBadRequest:
        # bot not admin in the gate channel, or bad id — fail open with a warning
        log.warning("force-sub check failed for channel %s; allowing through", FORCE_SUB)
        return True


def _join_keyboard(payload: str) -> InlineKeyboardMarkup:
    gate = FORCE_SUB
    url = f"https://t.me/{gate.lstrip('@')}" if isinstance(gate, str) else None
    rows = []
    if url:
        rows.append([InlineKeyboardButton(text="📢 Join channel", url=url)])
    rows.append([InlineKeyboardButton(text="✅ I've joined", callback_data=f"chk:{payload}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _serve(bot: Bot, chat_id: int, message_id: int) -> None:
    row = catalog.get(message_id)
    if not row:
        await bot.send_message(chat_id, "Sorry, that file is no longer available.")
        return
    try:
        sent = await bot.copy_message(
            chat_id=chat_id, from_chat_id=STORAGE, message_id=message_id
        )
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.error("copy_message failed: %s", e)
        await bot.send_message(
            chat_id,
            "Couldn't fetch that file. (The bot must be an admin of the storage channel.)",
        )
        return
    if AUTO_DELETE > 0:
        await bot.send_message(
            chat_id,
            f"⚠️ This file will be deleted in {AUTO_DELETE // 60 or 1} min. "
            f"Forward it somewhere to keep it.",
        )
        asyncio.create_task(_auto_delete(bot, chat_id, sent.message_id))


async def _auto_delete(bot: Bot, chat_id: int, message_id: int) -> None:
    await asyncio.sleep(AUTO_DELETE)
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest:
        pass


# --------------------------------------------------------------------------
# handlers
# --------------------------------------------------------------------------
@dp.message(CommandStart(deep_link=True))
async def start_deeplink(message: Message, command: CommandObject, bot: Bot) -> None:
    catalog.add_user(message.from_user.id, message.from_user.username,
                     message.from_user.language_code)
    payload = command.args or ""
    if payload.startswith("get_"):
        mid = payload[4:]
        if not mid.isdigit():
            await message.answer("Invalid link.")
            return
        if not await _is_subscribed(bot, message.from_user.id):
            await message.answer("Join the channel first to download:",
                                 reply_markup=_join_keyboard(payload))
            return
        await _serve(bot, message.chat.id, int(mid))
    else:
        await start(message)


@dp.message(CommandStart())
async def start(message: Message) -> None:
    catalog.add_user(message.from_user.id, message.from_user.username,
                     message.from_user.language_code)
    await message.answer(
        "👋 Send me a title and I'll find it.\n\n"
        f"You can also search inline anywhere: <code>@{BOT_USERNAME} your query</code>"
    )


@dp.message(Command("stats"))
async def stats(message: Message) -> None:
    if message.from_user.id not in ADMINS:
        return
    await message.answer(
        f"📊 Files: <b>{catalog.file_count()}</b>\nUsers: <b>{catalog.user_count()}</b>"
    )


@dp.message(Command("broadcast"))
async def broadcast(message: Message, command: CommandObject, bot: Bot) -> None:
    if message.from_user.id not in ADMINS:
        return
    text = command.args
    if not text:
        await message.answer("Usage: /broadcast &lt;message&gt;")
        return
    sent = failed = 0
    for uid in list(catalog.all_user_ids()):
        try:
            await bot.send_message(uid, text)
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            failed += 1
        await asyncio.sleep(0.05)  # stay under broadcast rate limits
    await message.answer(f"Broadcast done. sent={sent} failed={failed}")


@dp.message(F.text & ~F.text.startswith("/"))
async def search(message: Message) -> None:
    catalog.add_user(message.from_user.id, message.from_user.username,
                     message.from_user.language_code)
    rows = catalog.search(message.text, limit=RESULTS_LIMIT)
    if not rows:
        await message.answer("No matches found. Try a different spelling or fewer words.")
        return
    await message.answer(
        f"Found {len(rows)} result(s) for <b>{message.text}</b>:",
        reply_markup=_results_keyboard(rows),
    )


@dp.callback_query(F.data.startswith("get:"))
async def on_get(call: CallbackQuery, bot: Bot) -> None:
    mid = int(call.data.split(":", 1)[1])
    if not await _is_subscribed(bot, call.from_user.id):
        await call.message.answer("Join the channel first to download:",
                                  reply_markup=_join_keyboard(f"get_{mid}"))
        await call.answer()
        return
    await _serve(bot, call.message.chat.id, mid)
    await call.answer()


@dp.callback_query(F.data.startswith("chk:"))
async def on_check_sub(call: CallbackQuery, bot: Bot) -> None:
    payload = call.data.split(":", 1)[1]
    if not await _is_subscribed(bot, call.from_user.id):
        await call.answer("You haven't joined yet.", show_alert=True)
        return
    if payload.startswith("get_") and payload[4:].isdigit():
        await _serve(bot, call.message.chat.id, int(payload[4:]))
    await call.answer()


@dp.inline_query()
async def inline_search(query: InlineQuery) -> None:
    rows = catalog.search(query.query, limit=RESULTS_LIMIT) if query.query else []
    results = []
    for r in rows:
        deep_link = f"https://t.me/{BOT_USERNAME}?start=get_{r['message_id']}"
        results.append(
            InlineQueryResultArticle(
                id=str(r["message_id"]),
                title=_label(r),
                description=(r["file_name"] or r["caption"] or "")[:80],
                input_message_content=InputTextMessageContent(
                    message_text=f"📥 Get <b>{_label(r)}</b>:\n{deep_link}"
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="📥 Download", url=deep_link)]]
                ),
            )
        )
    await query.answer(results, cache_time=5, is_personal=True)


async def main() -> None:
    global BOT_USERNAME
    bot = Bot(CFG["bot_token"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    me = await bot.get_me()
    BOT_USERNAME = me.username
    log.info("bot @%s online; catalog has %d files", BOT_USERNAME, catalog.file_count())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
