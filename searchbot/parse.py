"""Best-effort extraction of title / year / quality / language from a Telegram
message's filename and caption. Release names are messy; this is heuristic and
intended to be tuned against your actual channel's naming.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")

_QUALITY_RE = re.compile(
    r"\b(2160p|1080p|720p|480p|360p|4k|uhd|hdrip|web[- ]?dl|web[- ]?rip|"
    r"bluray|brrip|bdrip|dvdrip|hdtv|cam|hdcam|predvd)\b",
    re.IGNORECASE,
)

# Order matters a little (multi-word first); all matched case-insensitively.
_LANGUAGES = [
    "dual audio", "multi audio", "hindi", "english", "tamil", "telugu",
    "malayalam", "kannada", "bengali", "punjabi", "marathi", "gujarati",
    "urdu", "korean", "japanese", "chinese", "spanish", "french",
]

# separators commonly used in release names
_SEP_RE = re.compile(r"[._\-\[\]\(\)]+")
_EXT_RE = re.compile(r"\.(mkv|mp4|avi|mov|wmv|flv|m4v|webm|ts)$", re.IGNORECASE)


@dataclass
class ParsedMeta:
    title: str
    year: Optional[int]
    quality: Optional[str]
    language: Optional[str]


def _clean(text: str) -> str:
    return _SEP_RE.sub(" ", text).strip()


def parse(file_name: Optional[str], caption: Optional[str]) -> ParsedMeta:
    """Derive searchable metadata. ``file_name`` is preferred for the title
    (release names are denser); ``caption`` is used as a fallback and for the
    language/quality scan."""
    file_name = (file_name or "").strip()
    caption = (caption or "").strip()
    haystack = f"{file_name} {caption}"

    year_m = _YEAR_RE.search(haystack)
    year = int(year_m.group(1)) if year_m else None

    quality_m = _QUALITY_RE.search(haystack)
    quality = quality_m.group(1).lower() if quality_m else None

    language = None
    low = haystack.lower()
    for lang in _LANGUAGES:
        if lang in low:
            language = lang.title()
            break

    # Title: take the filename up to the year (if any), else the first line of
    # the caption, else the whole filename.
    source = _EXT_RE.sub("", file_name) if file_name else caption.split("\n", 1)[0]
    if year_m and year_m.group(1) in source:
        source = source.split(year_m.group(1), 1)[0]
    title = _clean(source)
    if not title:
        title = _clean(caption.split("\n", 1)[0]) or "Untitled"

    return ParsedMeta(title=title, year=year, quality=quality, language=language)
