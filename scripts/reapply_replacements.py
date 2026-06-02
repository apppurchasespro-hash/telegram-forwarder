"""Re-apply a pair's replacement rules to already-forwarded dest messages.

Used when rules were added AFTER the bulk forward — the existing dest msgs
still have the un-stripped source text. This script:

  1. Loads pair config + message_map for the pair
  2. For each src→dst mapping, fetches src text in batches of 100
  3. Runs `apply_replacements` on the src text
  4. If transformed != original → edits the dst msg in place via Telethon

Skips msgs where the rules wouldn't change anything (no API call wasted).
Handles FloodWaitError + MessageNotModifiedError. Resumable via a checkpoint
file — re-running picks up where it left off.

Usage:
    py -3.11 scripts/reapply_replacements.py --pair "MOVIES AND SERIES - clone of eih paid"
    py -3.11 scripts/reapply_replacements.py --pair "..." --dry-run
    py -3.11 scripts/reapply_replacements.py --pair "..." --min-src-id 55000
"""
from __future__ import annotations
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, MessageNotModifiedError, MessageAuthorRequiredError

from automate import apply_replacements


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True, help="exact pair name from pairs.json")
    ap.add_argument("--pairs-file", default="data_acct2/pairs.json")
    ap.add_argument("--map-file", default="data_acct2/message_map.json")
    ap.add_argument("--checkpoint", default=None, help="checkpoint file (default: data_acct2/reapply_<pair>.json)")
    ap.add_argument("--min-src-id", type=int, default=0, help="only process src ids >= this (useful to skip early msgs that have no branding)")
    ap.add_argument("--max-src-id", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="count without editing")
    ap.add_argument("--rate", type=float, default=0.25, help="seconds to sleep between edits (default 0.25 = 4/sec)")
    ap.add_argument("--batch", type=int, default=100, help="src msgs fetched per get_messages call (max 100)")
    args = ap.parse_args()

    cfg = json.loads((ROOT / "config.json").read_text())
    sess = (ROOT / "secrets" / "acct2_session_string.txt").read_text().strip()
    pairs_cfg = json.loads((ROOT / args.pairs_file).read_text(encoding="utf-8"))
    msg_map = json.loads((ROOT / args.map_file).read_text(encoding="utf-8"))

    pair = next((p for p in pairs_cfg["pairs"] if p["name"] == args.pair), None)
    if not pair:
        print(f"ERROR: pair '{args.pair}' not found in {args.pairs_file}", file=sys.stderr)
        return 1

    rules = pair.get("replacements") or []
    if not rules:
        print(f"ERROR: pair '{args.pair}' has no replacements configured")
        return 1

    src_id = pair["source"]
    dst_id = pair["dest"]
    pair_map = msg_map.get(args.pair, {})
    if not pair_map:
        print(f"ERROR: no mappings for pair '{args.pair}'")
        return 1

    # Filter src ids by range
    src_ids = sorted(int(k) for k in pair_map.keys())
    if args.min_src_id:
        src_ids = [s for s in src_ids if s >= args.min_src_id]
    if args.max_src_id:
        src_ids = [s for s in src_ids if s <= args.max_src_id]

    # Checkpoint: skip already-processed src ids
    ckpt_path = Path(args.checkpoint) if args.checkpoint else ROOT / "data_acct2" / f"reapply_{args.pair.replace(' ', '_')[:40]}.json"
    ckpt = {"processed": [], "edited": 0, "skipped_unchanged": 0, "errors": 0, "no_caption": 0, "no_change": 0}
    if ckpt_path.exists():
        try:
            ckpt = json.loads(ckpt_path.read_text(encoding="utf-8"))
            already = set(ckpt.get("processed", []))
            before = len(src_ids)
            src_ids = [s for s in src_ids if s not in already]
            print(f"Checkpoint loaded: {len(already):,} already processed, {before - len(src_ids):,} skipped. Remaining: {len(src_ids):,}")
        except Exception as e:
            print(f"Warning: couldn't load checkpoint: {e}", file=sys.stderr)

    print(f"\nPair:       {args.pair}")
    print(f"Source:     {src_id}")
    print(f"Dest:       {dst_id}")
    print(f"Rules:      {len(rules)}")
    print(f"Mappings:   {len(pair_map):,} total, {len(src_ids):,} to process")
    print(f"Mode:       {'DRY RUN' if args.dry_run else 'EDIT IN PLACE'}")
    print(f"Rate:       {args.rate}s between edits ({1/args.rate:.1f}/sec)")
    print()

    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorized", file=sys.stderr)
        return 1

    start = time.time()
    last_save = start
    processed_in_session = 0

    try:
        for chunk_idx, chunk_src_ids in enumerate(chunks(src_ids, args.batch)):
            # Fetch a batch of source messages — get_messages accepts up to 100 ids.
            src_msgs = await client.get_messages(src_id, ids=chunk_src_ids)
            for sid, src_msg in zip(chunk_src_ids, src_msgs):
                ckpt["processed"].append(sid)
                processed_in_session += 1
                if src_msg is None:
                    # Source msg deleted; can't recompute. Leave dest as-is.
                    continue
                src_text = src_msg.message or ""
                if not src_text:
                    ckpt["no_caption"] += 1
                    continue
                new_text = apply_replacements(src_text, rules)
                if new_text == src_text:
                    ckpt["no_change"] += 1
                    continue
                if args.dry_run:
                    ckpt["edited"] += 1
                    continue

                dst_msg_id = pair_map.get(str(sid))
                if not dst_msg_id:
                    continue
                try:
                    await client.edit_message(dst_id, dst_msg_id, new_text, parse_mode=None)
                    ckpt["edited"] += 1
                except FloodWaitError as fw:
                    print(f"  ⏳ Flood wait {fw.seconds}s at src#{sid}")
                    await asyncio.sleep(fw.seconds + 1)
                    try:
                        await client.edit_message(dst_id, dst_msg_id, new_text, parse_mode=None)
                        ckpt["edited"] += 1
                    except Exception as e:
                        ckpt["errors"] += 1
                        print(f"  ✗ src#{sid} -> dst#{dst_msg_id} retry failed: {e}")
                except MessageNotModifiedError:
                    ckpt["skipped_unchanged"] += 1  # already had the cleaned caption
                except MessageAuthorRequiredError:
                    ckpt["errors"] += 1
                    print(f"  ✗ src#{sid} -> dst#{dst_msg_id}: not the message author (can't edit)")
                except Exception as e:
                    ckpt["errors"] += 1
                    print(f"  ✗ src#{sid} -> dst#{dst_msg_id}: {type(e).__name__}: {e}")
                await asyncio.sleep(args.rate)

            # Progress + checkpoint every batch
            now = time.time()
            elapsed = now - start
            rate = processed_in_session / max(elapsed, 0.001)
            remaining = (len(src_ids) - processed_in_session) / max(rate, 0.001)
            print(f"  [{chunk_idx+1}/{(len(src_ids)+args.batch-1)//args.batch}] "
                  f"processed={processed_in_session:,}  "
                  f"edited={ckpt['edited']:,}  "
                  f"unchanged={ckpt['no_change']:,}  "
                  f"errors={ckpt['errors']}  "
                  f"rate={rate:.1f}/s  ETA={remaining/60:.1f}m")

            # Save checkpoint every 30s
            if now - last_save > 30:
                ckpt_path.write_text(json.dumps(ckpt), encoding="utf-8")
                last_save = now
    finally:
        ckpt_path.write_text(json.dumps(ckpt), encoding="utf-8")
        await client.disconnect()

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"DONE in {elapsed/60:.1f} min")
    print(f"  edited:           {ckpt['edited']:,}")
    print(f"  no_change:        {ckpt['no_change']:,}  (rules wouldn't change anything)")
    print(f"  no_caption:       {ckpt['no_caption']:,}  (src msg has no text)")
    print(f"  skipped_unchanged:{ckpt['skipped_unchanged']:,}  (dest already clean)")
    print(f"  errors:           {ckpt['errors']:,}")
    print(f"Checkpoint: {ckpt_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
