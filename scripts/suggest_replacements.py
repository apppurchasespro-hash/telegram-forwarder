"""Sample messages from a source channel and suggest replacement rules.

Scans the latest N captions, extracts URLs / @mentions / repeated promo
lines, and prints suggested `replacements` config you can paste into the
dashboard's pair-form textarea.

Usage:
    py -3.11 scripts/suggest_replacements.py --src -1003612968908 --limit 500
"""
from __future__ import annotations
import argparse
import asyncio
import json
import re
import sys
from collections import Counter
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from telethon import TelegramClient
from telethon.sessions import StringSession

ROOT = Path(__file__).resolve().parent.parent

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
TME_RE = re.compile(r"(?<![/@\w.])t\.me/\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"@\w{3,}")
HASHTAG_RE = re.compile(r"#\w{3,}")
EMOJI_LINE_HINT = re.compile(r"^[^\w\s]{2,}")  # lines that start with multiple symbols/emoji — typical of branding banners


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=int, required=True, help="source channel id")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    cfg = json.loads((ROOT / "config.json").read_text())
    sess = (ROOT / "secrets" / "acct2_session_string.txt").read_text().strip()
    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorized", file=sys.stderr)
        return 1

    print(f"Scanning {args.limit} most recent messages from {args.src}...")
    captions = []
    n_with_text = 0
    async for m in client.iter_messages(args.src, limit=args.limit):
        if m.message:
            captions.append(m.message)
            n_with_text += 1
    await client.disconnect()

    print(f"Found {n_with_text} messages with text/caption out of {args.limit}\n")

    # URL extraction
    urls = Counter()
    tme = Counter()
    mentions = Counter()
    hashtags = Counter()
    # Repeated lines (likely branding banners)
    lines = Counter()
    for cap in captions:
        for u in URL_RE.findall(cap):
            # Normalize URL → domain/path-prefix so similar URLs cluster
            urls[u.rstrip(".,!?;:)]\"'")] += 1
        for t in TME_RE.findall(cap):
            tme[t.rstrip(".,!?;:)]\"'")] += 1
        for m in MENTION_RE.findall(cap):
            mentions[m] += 1
        for h in HASHTAG_RE.findall(cap):
            hashtags[h] += 1
        for ln in cap.splitlines():
            ln = ln.strip()
            if 3 <= len(ln) <= 80 and not URL_RE.search(ln):
                lines[ln] += 1

    # Domain rollup for URLs
    domains = Counter()
    for u, n in urls.items():
        mdom = re.match(r"https?://([^/]+)", u, re.I)
        if mdom:
            domains[mdom.group(1).lower()] += n

    print("=" * 70)
    print("TOP URL DOMAINS (top 10)")
    print("=" * 70)
    for d, n in domains.most_common(10):
        print(f"  {n:>4}x  {d}")

    print("\n" + "=" * 70)
    print("TOP @MENTIONS (top 10)")
    print("=" * 70)
    for m, n in mentions.most_common(10):
        print(f"  {n:>4}x  {m}")

    print("\n" + "=" * 70)
    print("TOP #HASHTAGS (top 10)")
    print("=" * 70)
    for h, n in hashtags.most_common(10):
        print(f"  {n:>4}x  {h}")

    print("\n" + "=" * 70)
    print("REPEATED LINES (likely banner/branding text, appearing >=3x, top 15)")
    print("=" * 70)
    repeated = [(l, n) for l, n in lines.most_common(50) if n >= 3]
    for l, n in repeated[:15]:
        print(f"  {n:>4}x  {l[:80]}")

    # SUGGESTIONS
    print("\n" + "=" * 70)
    print("SUGGESTED REPLACEMENT RULES (paste into dashboard textarea)")
    print("=" * 70)
    suggested = []
    suggested.append("# Remove all HTTP/HTTPS URLs")
    suggested.append("regex:https?://\\S+ =>")
    if tme:
        suggested.append("# Remove plain t.me/ links (no protocol)")
        suggested.append("regex:t\\.me/\\S+ =>")
    if mentions:
        suggested.append("# Remove @-mentions (channel handles)")
        suggested.append("regex:@\\w+ =>")
    if hashtags:
        suggested.append("# Remove #hashtags")
        suggested.append("regex:#\\w+ =>")
    # Add literal lines repeated >=5 times
    branding = [(l, n) for l, n in repeated if n >= 5 and len(l) >= 5]
    if branding:
        suggested.append("# Repeated branding/banner lines (literal removal)")
        for l, n in branding[:10]:
            suggested.append(f"{l} =>")

    for s in suggested:
        print(s)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
