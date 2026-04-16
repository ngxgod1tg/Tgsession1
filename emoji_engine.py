"""
emoji_engine.py — Premium Emoji Engine for Telegram Bots (Telethon)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Loads emoji_db.json once at startup and provides:
  • e(name)       → fallback emoji string for keyboards
  • get_flag(x)   → Unicode flag from ISO code or country name
  • send(...)     → send Telethon message with premium emoji overlay
  • respond(...)  → respond to event with premium emoji overlay

Edit emoji_db.json to change ANY emoji. Zero code changes needed.
"""

import json
import logging
from pathlib import Path
from telethon.tl.types import MessageEntityCustomEmoji

log = logging.getLogger("emoji_engine")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1) LOAD DATABASE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_DB_PATH = Path(__file__).parent / "emoji_db.json"

def _load_db() -> dict:
    """Load the emoji database from JSON."""
    with open(_DB_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

_db = _load_db()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2) EMOJI VALIDATION — filter non-emoji characters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Telegram's MessageEntityCustomEmoji ONLY works on actual emoji
#  characters. Non-emoji symbols like •, ✓, ₹, ₮, ━ will cause
#  ENTITY_TEXT_INVALID error.

# Characters that CANNOT have premium emoji overlay
_NON_EMOJI_CHARS = frozenset({
    "•", "✓", "━", "₹", "₮", "·", "―", "—", "–", "│", "┃",
    "►", "▸", "▪", "▫", "■", "□", "●", "○", "◆", "◇",
})

def _is_valid_emoji(text: str) -> bool:
    """Check if text is a real emoji that can be overlaid with CustomEmoji.
    
    Must be an actual emoji character (not punctuation/symbol).
    Valid: 🛒, ✅, ⚡, 🇮🇳, ❌, ⭐, 💰, ☀️, ⚠️, ↗️
    Invalid: •, ✓, ━, ₹, ₮, ·
    """
    if text in _NON_EMOJI_CHARS:
        return False
    # Check first code point — real emojis are typically >= 0x200D or in specific ranges
    cp = ord(text[0])
    # Emoji ranges that are safe:
    # U+2194-U+2BFF: Arrows, misc symbols, dingbats (most are emoji)
    # U+1F000-U+1FFFF: Supplementary emoji
    # U+FE00-U+FE0F: Variation selectors (part of emoji sequences)
    # Regional indicators: U+1F1E6-U+1F1FF
    if cp >= 0x1F000:  # Supplementary emoji plane
        return True
    if 0x1F1E6 <= cp <= 0x1F1FF:  # Regional indicator symbols (flags)
        return True
    # Common emoji in BMP with variation selectors
    if len(text) >= 2 and text[-1] == '\ufe0f':  # Has emoji presentation selector
        return True
    # Known valid single-char emoji ranges
    if cp in (0x2705, 0x2714, 0x2716, 0x274C, 0x274E, 0x2728, 0x2734, 0x2733,
              0x2B50, 0x2B55, 0x2B1B, 0x2B1C, 0x2764, 0x2763,
              0x23F0, 0x23F1, 0x23F3, 0x23F8, 0x23F9, 0x23FA,  # Timer/stopwatch
              0x2615, 0x2611, 0x2622, 0x2623, 0x2626, 0x262A,
              0x2660, 0x2663, 0x2665, 0x2666,  # Card suits
              0x2702, 0x2708, 0x2709, 0x270A, 0x270B, 0x270C, 0x270D, 0x270F,
              0x2712, 0x2744, 0x2747, 0x274C, 0x2753, 0x2754, 0x2755, 0x2757,
              0x2795, 0x2796, 0x2797, 0x27A1, 0x27B0,
              0x2934, 0x2935, 0x25AA, 0x25AB, 0x25B6, 0x25C0,
              0x25FB, 0x25FC, 0x25FD, 0x25FE,
              0x2600, 0x2601, 0x2602, 0x2603, 0x2604,  # Weather
              0x260E, 0x2614, 0x2618, 0x261D,
              0x2620, 0x2639, 0x263A, 0x2640, 0x2642,
              0x2648, 0x2649, 0x264A, 0x264B, 0x264C, 0x264D,
              0x264E, 0x264F, 0x2650, 0x2651, 0x2652, 0x2653,
              0x265F, 0x2668, 0x267B, 0x267E, 0x267F,
              0x2692, 0x2693, 0x2694, 0x2695, 0x2696, 0x2697,
              0x2699, 0x269B, 0x269C,
              0x26A0, 0x26A1, 0x26A7, 0x26AA, 0x26AB,
              0x26B0, 0x26B1, 0x26BD, 0x26BE,
              0x26C4, 0x26C5, 0x26C8, 0x26CE, 0x26CF,
              0x26D1, 0x26D3, 0x26D4,
              0x26E9, 0x26EA,
              0x26F0, 0x26F1, 0x26F2, 0x26F3, 0x26F4, 0x26F5,
              0x26F7, 0x26F8, 0x26F9, 0x26FA, 0x26FD):
        return True
    # Broader BMP emoji ranges
    if 0x2194 <= cp <= 0x21AA:  # Arrows
        return True
    if 0x231A <= cp <= 0x23FA:  # Misc technical (watch, hourglass, etc.)
        return True
    if 0x2934 <= cp <= 0x2935:
        return True
    return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3) BUILD LOOKUP TABLES (computed once at import time)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# name → (fallback_emoji, document_id_int_or_None)
_EMOJIS: dict[str, tuple[str, int | None]] = {}
for name, (fallback, doc_id_str) in _db.get("emojis", {}).items():
    _EMOJIS[name] = (fallback, int(doc_id_str) if doc_id_str else None)

# ISO code → premium document_id (int)
_FLAG_IDS: dict[str, int] = {}
for iso, doc_id_str in _db.get("flags", {}).items():
    _FLAG_IDS[iso.upper()] = int(doc_id_str)

# country name / alias → ISO code
_ALIASES: dict[str, str] = {}
for alias, iso in _db.get("country_aliases", {}).items():
    _ALIASES[alias.lower()] = iso.upper()
# Also add ISO codes as self-references
for iso in _FLAG_IDS:
    _ALIASES[iso.lower()] = iso

# Unicode flag emoji → premium document_id
_FLAG_PREMIUM: dict[str, int] = {}
for iso, doc_id in _FLAG_IDS.items():
    flag = "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in iso)
    _FLAG_PREMIUM[flag] = doc_id

# Reverse: fallback emoji string → document_id (for auto-detection in text)
# ONLY include entries where the fallback is a valid emoji character
_EMOJI_PREMIUM: dict[str, int] = {}
for _name, (fallback, doc_id) in _EMOJIS.items():
    if doc_id and fallback not in _EMOJI_PREMIUM and _is_valid_emoji(fallback):
        _EMOJI_PREMIUM[fallback] = doc_id

_skipped = sum(1 for _, (fb, did) in _EMOJIS.items() if did and not _is_valid_emoji(fb))
if _skipped:
    log.debug(f"Skipped {_skipped} non-emoji chars from premium overlay")

# Pre-sort by length descending so longer emojis match first (e.g. ⚠️ before ⚠)
_SORTED_EMOJIS = sorted(_EMOJI_PREMIUM.items(), key=lambda x: len(x[0]), reverse=True)
_SORTED_FLAGS = sorted(_FLAG_PREMIUM.items(), key=lambda x: len(x[0]), reverse=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4) PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def e(name: str) -> str:
    """Get fallback emoji by name. For keyboards and inline buttons.
    
    Usage: e('buy') → '🛒'
    """
    entry = _EMOJIS.get(name)
    return entry[0] if entry else "❓"


def get_flag(text: str) -> str:
    """Resolve country name or ISO code to Unicode flag emoji.
    
    Usage:
        get_flag('india')  → '🇮🇳'
        get_flag('IN')     → '🇮🇳'
        get_flag('us')     → '🇺🇸'
    """
    key = text.strip().lower()
    iso = _ALIASES.get(key)
    if not iso:
        return "🌍"  # fallback globe
    return "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in iso)


def get_flag_doc_id(text: str) -> int | None:
    """Get the premium document_id for a country flag."""
    key = text.strip().lower()
    iso = _ALIASES.get(key)
    if iso:
        return _FLAG_IDS.get(iso)
    return None


def resolve_country(raw: str) -> tuple[str, str] | None:
    """Resolve user input to (canonical_name, flag_emoji).
    
    Accepts: full name, alias, ISO code.
    Returns None if unrecognized.
    """
    key = raw.strip().lower()
    if not key or len(key) < 2:
        return None
    iso = _ALIASES.get(key)
    if not iso:
        return None
    # Find canonical name (first alias that maps to this ISO)
    canonical = key
    for alias, code in _ALIASES.items():
        if code == iso and len(alias) > 2:
            canonical = alias
            break
    flag = "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in iso)
    return (canonical, flag)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  5) UTF-16 OFFSET ENGINE (Telegram uses UTF-16 offsets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _utf16_len(text: str) -> int:
    """Length of string in UTF-16 code units (Telegram's offset system)."""
    return len(text.encode("utf-16-le")) // 2


def _find_offsets(text: str, target: str) -> list[tuple[int, int]]:
    """Find all (utf16_offset, utf16_length) of target in text."""
    results = []
    start = 0
    tgt_len = _utf16_len(target)
    while True:
        idx = text.find(target, start)
        if idx == -1:
            break
        results.append((_utf16_len(text[:idx]), tgt_len))
        start = idx + len(target)
    return results


def apply_premium(parsed_text: str, entities: list) -> bool:
    """Scan parsed_text and append premium emoji entities.
    
    Modifies `entities` list in-place.
    Returns True if any premium emojis were added.
    """
    has_premium = False

    # 1) Regular emojis (already filtered to valid emoji chars only)
    for fallback, doc_id in _SORTED_EMOJIS:
        if fallback not in parsed_text:
            continue
        for offset, length in _find_offsets(parsed_text, fallback):
            entities.append(MessageEntityCustomEmoji(
                offset=offset, length=length, document_id=doc_id,
            ))
            has_premium = True

    # 2) Country flag emojis
    for flag_emoji, doc_id in _SORTED_FLAGS:
        if flag_emoji not in parsed_text:
            continue
        for offset, length in _find_offsets(parsed_text, flag_emoji):
            entities.append(MessageEntityCustomEmoji(
                offset=offset, length=length, document_id=doc_id,
            ))
            has_premium = True

    return has_premium


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  6) TELETHON SEND HELPERS (with error fallback)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_text(text: str, parse_mode: str) -> tuple[str, list]:
    """Parse markdown/HTML to plain text + formatting entities."""
    if parse_mode == "html":
        from telethon.extensions import html as _html
        parsed, ents = _html.parse(text)
    elif parse_mode == "md":
        from telethon.extensions import markdown as _md
        parsed, ents = _md.parse(text)
    else:
        parsed, ents = text, []
    return parsed, ents if ents else []


async def send(bot, target, text: str, buttons=None, parse_mode: str = "md"):
    """Send a message with automatic premium emoji overlay.
    
    Falls back to normal send if premium entities cause an error.
    """
    parsed, entities = _parse_text(text, parse_mode)

    if apply_premium(parsed, entities):
        try:
            await bot.send_message(
                target, parsed,
                formatting_entities=entities, buttons=buttons,
            )
            return
        except Exception as ex:
            log.warning(f"Premium send failed ({ex}), falling back to normal")
    
    await bot.send_message(
        target, text,
        parse_mode=parse_mode, buttons=buttons,
    )


async def respond(event, text: str, buttons=None, parse_mode: str = "md"):
    """Respond to an event with automatic premium emoji overlay.
    
    Falls back to normal respond if premium entities cause an error.
    """
    parsed, entities = _parse_text(text, parse_mode)

    if apply_premium(parsed, entities):
        try:
            await event.respond(
                parsed, formatting_entities=entities, buttons=buttons,
            )
            return
        except Exception as ex:
            log.warning(f"Premium respond failed ({ex}), falling back to normal")
    
    await event.respond(
        text, parse_mode=parse_mode, buttons=buttons,
    )


async def edit(event, text: str, buttons=None, parse_mode: str = "md"):
    """Edit an inline message with automatic premium emoji overlay.
    
    Falls back to normal edit if premium entities cause an error.
    """
    parsed, entities = _parse_text(text, parse_mode)

    try:
        if apply_premium(parsed, entities):
            try:
                await event.edit(
                    parsed, formatting_entities=entities, buttons=buttons,
                )
                return
            except Exception:
                pass  # Fall through to normal edit
        await event.edit(
            text, parse_mode=parse_mode, buttons=buttons,
        )
    except Exception:
        pass  # Silently ignore "message not modified" and other errors


def reload_db():
    """Hot-reload emoji_db.json without restarting bot."""
    global _db, _EMOJIS, _FLAG_IDS, _ALIASES, _FLAG_PREMIUM
    global _EMOJI_PREMIUM, _SORTED_EMOJIS, _SORTED_FLAGS
    
    _db = _load_db()
    
    _EMOJIS.clear()
    for name, (fallback, doc_id_str) in _db.get("emojis", {}).items():
        _EMOJIS[name] = (fallback, int(doc_id_str) if doc_id_str else None)
    
    _FLAG_IDS.clear()
    for iso, doc_id_str in _db.get("flags", {}).items():
        _FLAG_IDS[iso.upper()] = int(doc_id_str)
    
    _ALIASES.clear()
    for alias, iso in _db.get("country_aliases", {}).items():
        _ALIASES[alias.lower()] = iso.upper()
    for iso in _FLAG_IDS:
        _ALIASES[iso.lower()] = iso
    
    _FLAG_PREMIUM.clear()
    for iso, doc_id in _FLAG_IDS.items():
        flag = "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in iso)
        _FLAG_PREMIUM[flag] = doc_id
    
    _EMOJI_PREMIUM.clear()
    for _name, (fallback, doc_id) in _EMOJIS.items():
        if doc_id and fallback not in _EMOJI_PREMIUM and _is_valid_emoji(fallback):
            _EMOJI_PREMIUM[fallback] = doc_id
    
    _SORTED_EMOJIS = sorted(_EMOJI_PREMIUM.items(), key=lambda x: len(x[0]), reverse=True)
    _SORTED_FLAGS = sorted(_FLAG_PREMIUM.items(), key=lambda x: len(x[0]), reverse=True)
