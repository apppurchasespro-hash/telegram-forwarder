#!/usr/bin/env python3
"""
Telegram Downloader CLI — Telethon version

Usage:
    py -3.11 cli.py list-chats
    py -3.11 cli.py list-topics --chat "-1002460585809"
    py -3.11 cli.py forward-topic --source "-1002460585809" --topic 1 --dest "-1003951264037"
    py -3.11 cli.py download --chat "-1002460585809" --type documents
    py -3.11 cli.py export --chat "-1002460585809" --output messages.json
    py -3.11 cli.py forward --source "-1002460585809" --dest "-1003951264037"
"""

import argparse
import asyncio
import sys

from downloader import TelegramDownloader, load_config


def format_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def print_progress(progress):
    bar_width = 30
    filled = int(bar_width * progress.downloaded / max(progress.total_size, 1))
    bar = "█" * filled + "░" * (bar_width - filled)
    name = progress.filename[:35].ljust(35)
    speed = f"{format_size(int(progress.speed))}/s"
    if progress.status == "completed":
        print(f"\r  ✓ {name} [{bar}] Done!          ")
    elif progress.status == "failed":
        print(f"\r  ✗ {name} — Failed")
    else:
        print(f"\r  ⬇ {name} [{bar}] {progress.percent}% {speed}", end="", flush=True)


def print_message_progress(count, limit):
    if limit:
        print(f"\r  📋 Scanning: {count}/{limit}", end="", flush=True)
    else:
        print(f"\r  📋 Scanning: {count}", end="", flush=True)


async def cmd_list_chats(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chats = await dl.list_chats(limit=args.limit)
        if args.type:
            chats = [c for c in chats if c["type"] == args.type]

        print(f"\n{'─'*90}")
        print(f"  {'#':<4} {'ID':<16} {'Type':<12} {'Name':<30} {'Username':<20}")
        print(f"{'─'*90}")
        for i, chat in enumerate(chats, 1):
            username = f"@{chat['username']}" if chat["username"] else "—"
            print(f"  {i:<4} {str(chat['id']):<16} {chat['type']:<12} {chat['title'][:28]:<30} {username:<20}")
        print(f"{'─'*90}")
        print(f"  Total: {len(chats)} chats\n")


async def cmd_list_topics(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        topics = await dl.list_topics(chat_id)
        entity = await dl.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", str(chat_id))

        print(f"\n📋 Topics in: {chat_name}")
        print(f"{'─'*60}")
        print(f"  {'#':<4} {'Topic ID':<12} {'Name':<30}")
        print(f"{'─'*60}")
        for i, topic in enumerate(topics, 1):
            print(f"  {i:<4} {str(topic['id']):<12} {topic['title'][:28]:<30}")
        print(f"{'─'*60}")
        print(f"  Total: {len(topics)} topics\n")


async def cmd_forward_topic(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        source = args.source
        dest = args.dest
        try:
            source = int(source)
        except ValueError:
            pass
        try:
            dest = int(dest)
        except ValueError:
            pass

        await dl.forward_topic(
            source_id=source,
            topic_id=args.topic,
            dest_id=dest,
            forward_type=args.type,
            limit=args.limit,
            delay=args.delay,
        )


async def cmd_download(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        await dl.download_chat(
            chat_id=chat_id,
            download_type=args.type,
            limit=args.limit,
            on_progress=print_progress,
            on_message_progress=print_message_progress,
        )


async def cmd_export(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        chat_id = args.chat
        try:
            chat_id = int(chat_id)
        except ValueError:
            pass

        await dl.export_messages(
            chat_id=chat_id,
            output_file=args.output,
            limit=args.limit,
        )


async def cmd_forward(args):
    config = load_config()
    async with TelegramDownloader(config) as dl:
        source = args.source
        dest = args.dest
        try:
            source = int(source)
        except ValueError:
            pass
        try:
            dest = int(dest)
        except ValueError:
            pass

        await dl.forward_chat(
            source_id=source,
            dest_id=dest,
            forward_type=args.type,
            limit=args.limit,
            delay=args.delay,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Telegram Downloader (Telethon)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # list-chats
    lc = sub.add_parser("list-chats")
    lc.add_argument("--limit", type=int, default=50)
    lc.add_argument("--type", choices=["private", "group", "supergroup", "channel"], default=None)

    # list-topics
    lt = sub.add_parser("list-topics")
    lt.add_argument("--chat", required=True)

    # forward-topic
    ft = sub.add_parser("forward-topic")
    ft.add_argument("--source", required=True)
    ft.add_argument("--topic", required=True, type=int)
    ft.add_argument("--dest", required=True)
    ft.add_argument("--type", choices=["all", "media", "documents", "messages", "docs_and_text"], default="all")
    ft.add_argument("--limit", type=int, default=0)
    ft.add_argument("--delay", type=float, default=0.5)

    # download
    dl = sub.add_parser("download")
    dl.add_argument("--chat", required=True)
    dl.add_argument("--type", choices=["all", "media", "documents", "messages"], default="all")
    dl.add_argument("--limit", type=int, default=0)

    # export
    ex = sub.add_parser("export")
    ex.add_argument("--chat", required=True)
    ex.add_argument("--output", default="messages.json")
    ex.add_argument("--limit", type=int, default=0)

    # forward
    fw = sub.add_parser("forward")
    fw.add_argument("--source", required=True)
    fw.add_argument("--dest", required=True)
    fw.add_argument("--type", choices=["all", "media", "documents", "messages"], default="all")
    fw.add_argument("--limit", type=int, default=0)
    fw.add_argument("--delay", type=float, default=1.0)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "list-chats": cmd_list_chats,
        "list-topics": cmd_list_topics,
        "forward-topic": cmd_forward_topic,
        "download": cmd_download,
        "export": cmd_export,
        "forward": cmd_forward,
    }
    asyncio.run(commands[args.command](args))


if __name__ == "__main__":
    main()
