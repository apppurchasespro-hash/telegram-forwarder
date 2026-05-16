"""
Core Telegram Download Engine — Telethon version
No full-chat scanning: uses iter_messages(reply_to=topic_id) to fetch
topic messages directly from Telegram.
"""

import os
import json
import asyncio
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetForumTopicsRequest
from telethon.tl.types import ForumTopic, MessageMediaWebPage, DocumentAttributeFilename
from telethon.errors import FloodWaitError

if hasattr(sys.stdout, "reconfigure") and sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure") and sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).parent


def load_config(config_path: str = None) -> dict:
    # Env vars win — needed for cloud deployments (Railway, Docker, etc.)
    if os.environ.get("TELEGRAM_API_ID") and os.environ.get("TELEGRAM_API_HASH"):
        return {
            "api_id": int(os.environ["TELEGRAM_API_ID"]),
            "api_hash": os.environ["TELEGRAM_API_HASH"],
            "download_path": os.environ.get("DOWNLOAD_PATH", "./downloads"),
            "max_concurrent_downloads": int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3")),
        }
    path = Path(config_path) if config_path else BASE_DIR / "config.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            "Copy config.example.json to config.json and fill in your credentials, "
            "or set TELEGRAM_API_ID + TELEGRAM_API_HASH env vars."
        )
    with open(path, "r") as f:
        return json.load(f)


class DownloadProgress:
    def __init__(self, filename: str, total_size: int = 0):
        self.filename = filename
        self.total_size = total_size
        self.downloaded = 0
        self.speed = 0.0
        self.start_time = time.time()
        self.status = "pending"

    def update(self, current: int, total: int):
        self.downloaded = current
        self.total_size = total
        elapsed = time.time() - self.start_time
        self.speed = current / elapsed if elapsed > 0 else 0
        self.status = "downloading"

    def complete(self):
        self.status = "completed"
        self.downloaded = self.total_size

    def fail(self, error: str = ""):
        self.status = "failed"
        self.error = error

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "total_size": self.total_size,
            "downloaded": self.downloaded,
            "speed": self.speed,
            "status": self.status,
            "percent": round(self.downloaded / self.total_size * 100, 1) if self.total_size > 0 else 0,
        }


class TelegramDownloader:

    MIME_TO_EXT = {
        "application/pdf": ".pdf",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-powerpoint": ".ppt",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
        "application/zip": ".zip",
        "application/x-rar-compressed": ".rar",
        "application/x-7z-compressed": ".7z",
        "text/plain": ".txt",
        "video/mp4": ".mp4",
        "video/x-matroska": ".mkv",
        "audio/mpeg": ".mp3",
        "audio/ogg": ".ogg",
    }

    def __init__(self, config: Optional[dict] = None):
        if config is None:
            config = load_config()
        self.api_id = config["api_id"]
        self.api_hash = config["api_hash"]
        self.download_path = BASE_DIR / config.get("download_path", "downloads")
        self.max_concurrent = config.get("max_concurrent_downloads", 3)
        self.client: Optional[TelegramClient] = None
        self.progress_tracker: dict[str, DownloadProgress] = {}

    async def start(self):
        session_string = os.environ.get("TELETHON_SESSION_STRING")
        if session_string:
            session = StringSession(session_string)
        else:
            session = str(BASE_DIR / "tg_session")
        self.client = TelegramClient(session, self.api_id, self.api_hash)
        await self.client.start()
        me = await self.client.get_me()
        print(f"✓ Logged in as: {me.first_name} (@{me.username})")
        return self

    async def stop(self):
        if self.client:
            await self.client.disconnect()

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *args):
        await self.stop()

    # ─── Chats ───────────────────────────────────────────────────

    async def list_chats(self, limit: int = 50) -> list[dict]:
        chats = []
        async for dialog in self.client.iter_dialogs(limit=limit):
            entity = dialog.entity
            type_name = type(entity).__name__
            if "User" in type_name:
                chat_type = "private"
            elif "Channel" in type_name:
                chat_type = "channel" if entity.broadcast else "supergroup"
            else:
                chat_type = "group"

            chats.append({
                "id": dialog.id,
                "title": dialog.title or "Unknown",
                "username": getattr(entity, "username", None),
                "type": chat_type,
                "members_count": getattr(entity, "participants_count", None),
                "unread_count": dialog.unread_count,
            })
        return chats

    # ─── Topics ──────────────────────────────────────────────────

    async def list_topics(self, chat_id: int | str) -> list[dict]:
        entity = await self.client.get_entity(chat_id)
        result = await self.client(GetForumTopicsRequest(
            peer=entity,
            q="",
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100,
        ))
        topics = []
        for t in result.topics:
            if isinstance(t, ForumTopic):
                topics.append({
                    "id": t.id,
                    "title": t.title,
                    "unread_count": getattr(t, "unread_count", 0),
                    "top_message": t.top_message,
                })
        return topics

    # ─── Topic messages ──────────────────────────────────────────

    async def get_topic_messages(
        self,
        chat_id: int | str,
        topic_id: int,
        limit: int = 0,
    ) -> list:
        """
        Fetch messages from a forum topic.
        Uses iter_messages(reply_to=topic_id) — no full-chat scan needed.
        For the General topic (id=1), falls back to scanning since those
        messages have no reply_to.
        """
        messages = []
        count = 0

        if topic_id == 1:
            # General topic: messages have no reply_to, must scan all
            print("   General topic — scanning all messages (one-time)...")
            reply_to_map = {}
            async for msg in self.client.iter_messages(chat_id):
                count += 1
                if count % 500 == 0:
                    print(f"   Scanned {count}...", end="\r", flush=True)
                rid = msg.reply_to_msg_id if msg.reply_to else None
                reply_to_map[msg.id] = rid

            root_cache = {}
            def find_root(mid):
                if mid in root_cache:
                    return root_cache[mid]
                parent = reply_to_map.get(mid)
                if parent is None or parent not in reply_to_map:
                    root_cache[mid] = mid
                else:
                    root_cache[mid] = find_root(parent)
                return root_cache[mid]

            general_ids = {mid for mid in reply_to_map if reply_to_map.get(find_root(mid)) is None}

            # Fetch in batches
            id_list = sorted(general_ids)
            if limit:
                id_list = id_list[-limit:]
            batch_size = 200
            for i in range(0, len(id_list), batch_size):
                batch = id_list[i:i + batch_size]
                fetched = await self.client.get_messages(chat_id, ids=batch)
                messages.extend(m for m in fetched if m)
        else:
            # Named topic: Telethon fetches directly via messages.getReplies
            print(f"   Fetching topic {topic_id} messages directly...")
            async for msg in self.client.iter_messages(
                chat_id,
                reply_to=topic_id,
                limit=limit or None,
            ):
                count += 1
                if count % 200 == 0:
                    print(f"   Fetched {count}...", end="\r", flush=True)
                messages.append(msg)

        print(f"   Found {len(messages)} messages in topic")
        return messages

    # ─── Messages ────────────────────────────────────────────────

    async def get_messages(
        self,
        chat_id: int | str,
        limit: int = 100,
        offset_id: int = 0,
        media_only: bool = False,
    ) -> list:
        messages = []
        async for msg in self.client.iter_messages(chat_id, limit=limit, offset_id=offset_id):
            if media_only and not msg.media:
                continue
            messages.append(msg)
        return messages

    # ─── File helpers ─────────────────────────────────────────────

    def _get_filename(self, msg) -> str:
        date_str = msg.date.strftime("%Y%m%d_%H%M%S") if msg.date else "unknown"
        if msg.photo:
            return f"photo_{date_str}_{msg.id}.jpg"
        if msg.document:
            for attr in msg.document.attributes:
                if hasattr(attr, "file_name") and attr.file_name:
                    return attr.file_name
            ext = self.MIME_TO_EXT.get(msg.document.mime_type or "", "")
            return f"doc_{date_str}_{msg.id}{ext}"
        if msg.audio:
            return f"audio_{date_str}_{msg.id}.mp3"
        if msg.voice:
            return f"voice_{date_str}_{msg.id}.ogg"
        if msg.video:
            return f"video_{date_str}_{msg.id}.mp4"
        return f"file_{date_str}_{msg.id}"

    def _get_file_size(self, msg) -> int:
        if msg.document:
            return getattr(msg.document, "size", 0) or 0
        if msg.photo:
            sizes = getattr(msg.photo, "sizes", [])
            return max((getattr(s, "size", 0) for s in sizes), default=0)
        return 0

    def _get_download_dir(self, chat_name: str, category: str) -> Path:
        safe = "".join(c if c.isalnum() or c in " _-" else "_" for c in chat_name)
        d = self.download_path / safe / category
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _is_document(self, msg) -> bool:
        if not msg.document:
            return False
        mime = msg.document.mime_type or ""
        return not mime.startswith(("image/", "video/"))

    def _is_media(self, msg) -> bool:
        return bool(msg.photo or msg.video or msg.gif or msg.sticker or msg.video_note)

    # ─── Copy (download + re-upload, bypasses restrictions) ──────

    async def _copy_message_to(self, msg, dest_id: int | str, dest_topic: Optional[int] = None) -> bool:
        # dest_topic: forum supergroup topic id. None or 1 = General/no topic.
        reply_to = dest_topic if (dest_topic and dest_topic > 1) else None
        try:
            if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                temp_dir = BASE_DIR / "temp"
                temp_dir.mkdir(exist_ok=True)
                # Use msg.id-based temp name to avoid Windows-invalid chars in original filenames
                original_name = None
                ext = ""
                if msg.document:
                    for attr in msg.document.attributes:
                        if hasattr(attr, "file_name") and attr.file_name:
                            original_name = attr.file_name
                            ext = Path(attr.file_name).suffix
                            break
                    if not ext:
                        ext = self.MIME_TO_EXT.get(msg.document.mime_type or "", "")
                elif msg.photo:
                    ext = ".jpg"
                safe_temp = temp_dir / f"tmp_{msg.id}{ext}"
                temp_path = await self.client.download_media(msg, file=str(safe_temp))
                if not temp_path:
                    return False
                caption = msg.message or ""
                # Restore original filename on the sent file via attributes
                send_kwargs = {"caption": caption}
                if msg.entities:
                    send_kwargs["formatting_entities"] = msg.entities
                if original_name:
                    send_kwargs["force_document"] = True
                    send_kwargs["attributes"] = [DocumentAttributeFilename(original_name)]
                if reply_to:
                    send_kwargs["reply_to"] = reply_to
                await self.client.send_file(dest_id, temp_path, **send_kwargs)
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            elif msg.message:
                await self.client.send_message(
                    dest_id,
                    msg.message,
                    formatting_entities=msg.entities or None,
                    reply_to=reply_to,
                )
            else:
                return False
            return True
        except FloodWaitError as fw:
            print(f"\n  ⏳ Flood wait {fw.seconds}s...")
            await asyncio.sleep(fw.seconds)
            return await self._copy_message_to(msg, dest_id, dest_topic=dest_topic)
        except Exception as e:
            print(f"\n  ✗ Failed msg #{msg.id}: {e}")
            return False

    # ─── Forward topic ────────────────────────────────────────────

    async def forward_topic(
        self,
        source_id: int | str,
        topic_id: int,
        dest_id: int | str,
        forward_type: str = "all",
        limit: int = 0,
        delay: float = 0.5,
        on_progress: Optional[Callable] = None,
    ) -> dict:
        source = await self.client.get_entity(source_id)
        dest = await self.client.get_entity(dest_id)
        source_name = getattr(source, "title", str(source_id))
        dest_name = getattr(dest, "title", str(dest_id))

        topics = await self.list_topics(source_id)
        topic_name = next((t["title"] for t in topics if t["id"] == topic_id), f"Topic #{topic_id}")

        print(f"\n📨 Copying topic: {topic_name}")
        print(f"   From: {source_name} → {dest_name}")
        print(f"   Type: {forward_type} | Limit: {limit or 'unlimited'}")
        print(f"   Mode: Copy (bypasses forwarding restrictions)")

        stats = {
            "source": source_name,
            "topic": topic_name,
            "destination": dest_name,
            "total_scanned": 0,
            "forwarded": 0,
            "skipped": 0,
            "failed": 0,
        }

        all_messages = await self.get_topic_messages(source_id, topic_id, limit=limit)

        messages_to_send = []
        for msg in all_messages:
            stats["total_scanned"] += 1
            should_send = False
            if forward_type == "all":
                should_send = True
            elif forward_type == "media" and self._is_media(msg):
                should_send = True
            elif forward_type == "documents" and self._is_document(msg):
                should_send = True
            elif forward_type == "messages" and msg.message and not msg.media:
                should_send = True
            elif forward_type == "docs_and_text" and (self._is_document(msg) or (msg.message and not msg.media)):
                should_send = True

            if should_send:
                messages_to_send.append(msg)
            else:
                stats["skipped"] += 1

        messages_to_send.sort(key=lambda m: m.id)
        total = len(messages_to_send)
        print(f"   Sending {total} messages...\n")

        (BASE_DIR / "temp").mkdir(exist_ok=True)

        for i, msg in enumerate(messages_to_send, 1):
            success = await self._copy_message_to(msg, dest_id)
            if success:
                stats["forwarded"] += 1
            else:
                stats["failed"] += 1

            print(f"  {'✓' if success else '✗'} {i}/{total} (msg #{msg.id})", end="\r", flush=True)
            if on_progress:
                on_progress(i, total)
            await asyncio.sleep(delay)

        try:
            import shutil
            shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
        except Exception:
            pass

        print(f"\n\n{'='*50}")
        print(f"  ✓ Complete: {topic_name} → {dest_name}")
        print(f"  Scanned: {stats['total_scanned']} | Copied: {stats['forwarded']} | Failed: {stats['failed']}")
        print(f"{'='*50}\n")
        return stats

    # ─── Forward chat ──────────────────────────────────────────

    async def forward_chat(
        self,
        source_id: int | str,
        dest_id: int | str,
        forward_type: str = "all",
        limit: int = 0,
        delay: float = 1.0,
        on_progress: Optional[Callable] = None,
    ) -> dict:
        source = await self.client.get_entity(source_id)
        dest = await self.client.get_entity(dest_id)
        source_name = getattr(source, "title", str(source_id))
        dest_name = getattr(dest, "title", str(dest_id))
        protected = bool(getattr(source, "noforwards", False))

        print(f"\n📨 Forwarding: {source_name} → {dest_name}")
        if protected:
            print(f"   ⚠ Source is a protected chat — using copy-mode (download + re-upload)")
        stats = {"source": source_name, "destination": dest_name, "total_scanned": 0, "forwarded": 0, "skipped": 0, "failed": 0}

        messages_to_forward = []
        async for msg in self.client.iter_messages(source_id, limit=limit or None):
            stats["total_scanned"] += 1
            should = (
                forward_type == "all"
                or (forward_type == "media" and self._is_media(msg))
                or (forward_type == "documents" and self._is_document(msg))
                or (forward_type == "messages" and msg.message and not msg.media)
            )
            if should:
                messages_to_forward.append(msg)
            else:
                stats["skipped"] += 1

        messages_to_forward.sort(key=lambda m: m.id)
        total = len(messages_to_forward)
        print(f"   Found {total} to forward\n")

        if protected:
            (BASE_DIR / "temp").mkdir(exist_ok=True)
            for i, msg in enumerate(messages_to_forward, 1):
                ok = await self._copy_message_to(msg, dest_id)
                if ok:
                    stats["forwarded"] += 1
                else:
                    stats["failed"] += 1
                print(f"  {'✓' if ok else '✗'} {i}/{total} (msg #{msg.id})", end="\r", flush=True)
                if on_progress:
                    on_progress(i, total)
                await asyncio.sleep(delay)
            try:
                import shutil
                shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
            except Exception:
                pass
        else:
            batch_size = 100
            for i in range(0, total, batch_size):
                batch = messages_to_forward[i:i + batch_size]
                try:
                    await self.client.forward_messages(dest_id, batch, source_id)
                    stats["forwarded"] += len(batch)
                    print(f"  ✓ Forwarded {stats['forwarded']}/{total}", end="\r", flush=True)
                    if on_progress:
                        on_progress(stats["forwarded"], total)
                    if i + batch_size < total:
                        await asyncio.sleep(delay)
                except FloodWaitError as fw:
                    print(f"\n  ⏳ Flood wait {fw.seconds}s...")
                    await asyncio.sleep(fw.seconds)
                    try:
                        await self.client.forward_messages(dest_id, batch, source_id)
                        stats["forwarded"] += len(batch)
                    except Exception as e2:
                        stats["failed"] += len(batch)
                        print(f"  ✗ Retry failed: {e2}")
                except Exception as e:
                    stats["failed"] += len(batch)
                    print(f"\n  ✗ Batch failed: {e}")

        print(f"\n\n{'='*50}")
        print(f"  ✓ Forward Complete: {source_name} → {dest_name}")
        print(f"  Forwarded: {stats['forwarded']} | Failed: {stats['failed']}")
        print(f"{'='*50}\n")
        return stats

    # ─── Download chat ────────────────────────────────────────────

    async def download_chat(
        self,
        chat_id: int | str,
        download_type: str = "all",
        limit: int = 0,
        on_progress: Optional[Callable] = None,
        on_message_progress: Optional[Callable] = None,
    ) -> dict:
        entity = await self.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", getattr(entity, "first_name", str(chat_id)))
        print(f"\n📥 Downloading from: {chat_name} | Type: {download_type}")

        stats = {
            "chat_name": chat_name,
            "total_messages_scanned": 0,
            "media_downloaded": 0,
            "documents_downloaded": 0,
            "messages_exported": 0,
            "failed": 0,
            "skipped": 0,
            "downloaded_files": [],
            "exported_messages": [],
        }

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def download_one(msg):
            async with semaphore:
                if not msg.media or isinstance(msg.media, MessageMediaWebPage):
                    return None
                if download_type == "media" and not self._is_media(msg):
                    return None
                if download_type == "documents" and not self._is_document(msg):
                    return None

                category = "media" if self._is_media(msg) else "documents"
                filename = self._get_filename(msg)
                file_path = self._get_download_dir(chat_name, category) / filename

                if file_path.exists():
                    return str(file_path)

                progress = DownloadProgress(filename, self._get_file_size(msg))
                self.progress_tracker[filename] = progress

                def _cb(current, total):
                    progress.update(current, total)
                    if on_progress:
                        on_progress(progress)

                try:
                    result = await self.client.download_media(msg, file=str(file_path), progress_callback=_cb)
                    progress.complete()
                    return result
                except Exception as e:
                    progress.fail(str(e))
                    return None

        tasks = []
        async for msg in self.client.iter_messages(chat_id, limit=limit or None):
            stats["total_messages_scanned"] += 1
            if on_message_progress:
                on_message_progress(stats["total_messages_scanned"], limit)

            if download_type in ("messages", "all") and msg.message and not msg.media:
                stats["exported_messages"].append({
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else None,
                    "from": getattr(msg.sender, "first_name", None) if msg.sender else None,
                    "text": msg.message,
                    "reply_to": msg.reply_to_msg_id if msg.reply_to else None,
                })
                stats["messages_exported"] += 1

            if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                if download_type in ("all", "media", "documents"):
                    tasks.append(asyncio.create_task(download_one(msg)))

        if tasks:
            print(f"\n⬇ Downloading {len(tasks)} files...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    stats["failed"] += 1
                elif r is None:
                    stats["skipped"] += 1
                else:
                    stats["downloaded_files"].append(r)
                    if r.endswith((".jpg", ".mp4", ".gif", ".webp")):
                        stats["media_downloaded"] += 1
                    else:
                        stats["documents_downloaded"] += 1

        if stats["exported_messages"]:
            export_dir = self._get_download_dir(chat_name, "messages")
            with open(export_dir / "messages.json", "w", encoding="utf-8") as f:
                json.dump(stats["exported_messages"], f, ensure_ascii=False, indent=2)

        print(f"\n{'='*50}")
        print(f"  ✓ Download Complete: {chat_name}")
        print(f"  Scanned: {stats['total_messages_scanned']} | Docs: {stats['documents_downloaded']} | Media: {stats['media_downloaded']}")
        print(f"{'='*50}\n")
        return stats

    # ─── Export messages ──────────────────────────────────────────

    async def export_messages(
        self,
        chat_id: int | str,
        output_file: str = "messages.json",
        limit: int = 0,
    ) -> int:
        entity = await self.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", getattr(entity, "first_name", str(chat_id)))
        messages = []
        async for msg in self.client.iter_messages(chat_id, limit=limit or None):
            messages.append({
                "id": msg.id,
                "date": msg.date.isoformat() if msg.date else None,
                "from": getattr(msg.sender, "first_name", None) if msg.sender else None,
                "text": msg.message or "",
                "media_type": type(msg.media).__name__ if msg.media else None,
                "reply_to": msg.reply_to_msg_id if msg.reply_to else None,
                "views": getattr(msg, "views", None),
                "forwards": getattr(msg, "forwards", None),
            })
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({
                "chat_name": chat_name,
                "exported_at": datetime.now().isoformat(),
                "total_messages": len(messages),
                "messages": messages,
            }, f, ensure_ascii=False, indent=2)
        print(f"✓ Exported {len(messages)} messages to {output_file}")
        return len(messages)

    def get_progress(self) -> list[dict]:
        return [p.to_dict() for p in self.progress_tracker.values()]
