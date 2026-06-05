"""Search-and-serve bot package.

Two roles, deliberately split:

- The *indexer* (``indexer.py``) runs on your Telethon **user account** because
  bots cannot read a channel's message *history* via the Bot API. It walks the
  storage channel and writes a searchable catalog into SQLite.

- The *bot* (``bot.py``) runs on an aiogram **bot token**. It searches the
  catalog and serves files with ``copy_message`` (referencing the storage
  channel + message_id) — no re-upload, and it sidesteps the fact that
  MTProto file_ids are not reusable by Bot-API bots.
"""
