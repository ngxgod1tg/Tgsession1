"""
╔══════════════════════════════════════════════════════════════════╗
║       💎 PREMIUM TELEGRAM ACCOUNT MARKETPLACE                   ║
║       ReplyKeyboard-Only · Text-Based Navigation                ║
╠══════════════════════════════════════════════════════════════════╣
║  UI: ReplyKeyboard ONLY — zero inline buttons                   ║
║  Deposit:                                                        ║
║    🪙  Crypto (USDT) — OxaPay auto-verify via polling           ║
║    🏦  UPI/INR — Screenshot + admin approval                    ║
║  Marketplace:                                                    ║
║    🛒  Buy Telegram accounts with balance                        ║
║    📩  Auto OTP delivery (listens to 777000)                     ║
║    ⏳  24-hour auto-expiry with session logout                   ║
║  Admin Panel:                                                    ║
║    ➕ Add Account · 💳 Set UPI · 💰 Add Balance                 ║
║    📥 Deposit Requests · 📡 Broadcast · 📊 Stats                ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL:  pip install telethon aiohttp
RUN:      python crypto.py
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from keep import keep_alive
keep_alive() 

from telethon import TelegramClient, events, Button, types
from telethon.sessions import StringSession
# MessageEntityCustomEmoji is handled by emoji_engine
from telethon.errors import (
    MessageNotModifiedError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError,
    PhoneNumberInvalidError,
    PhoneNumberBannedError,
    UserDeactivatedBanError,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🗄️  MONGODB + BACKUP SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from database import get_db, ping_mongo, ensure_indexes, db_set_qr
from backup import check_and_recover, backup_all

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⚙️  CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise ValueError(f"Required environment variable '{name}' is not set. Add it to your .env file.")
    return val

API_ID              = int(_require_env("API_ID"))
API_HASH            = _require_env("API_HASH")
BOT_TOKEN           = _require_env("BOT_TOKEN")
OXAPAY_MERCHANT_KEY = _require_env("OXAPAY_KEY")
ADMIN_ID            = int(_require_env("ADMIN_ID"))
SUPPORT_LINK        = os.getenv("SUPPORT_LINK",    "https://t.me/+rJmN5Q2l8RA2MzBl")  # profile or group link

# OxaPay
OXAPAY_API_BASE          = "https://api.oxapay.com/v1"
INVOICE_LIFETIME_MINUTES = 30
POLL_INTERVAL_SECONDS    = 10
MIN_CREDIT_AMOUNT        = 0.1

# INR
UPI_ID             = "yourupi@upi"
INR_TO_USD_RATE    = 85.0
INR_PAYMENT_EXPIRY = 15 * 60
INR_MIN_AMOUNT     = 10
INR_MAX_AMOUNT     = 100000

# Session
SESSION_EXPIRY_HOURS = 24
DATA_FILE = Path("bot_store_data.json")
UPI_QR_FILE = Path("upi_qr.png")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📋  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎨  PREMIUM EMOJI ENGINE (modular — edit emoji_db.json only)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from emoji_engine import (
    e,                  # e('buy') → '🛒'
    get_flag,           # get_flag('india') → '🇮🇳'
    resolve_country,    # resolve_country('IN') → ('india', '🇮🇳')
    send as send_premium,       # await send_premium(bot, chat_id, text, buttons)
    respond as respond_premium, # await respond_premium(event, text, buttons)
    edit as safe_edit_premium,  # await safe_edit_premium(event, text, buttons)
    apply_premium,      # apply_premium(parsed_text, entities) → bool
    reload_db,          # reload_db() → hot-reload emoji_db.json
)

def normalize_country(raw: str) -> tuple[str, str] | None:
    """Resolve user input to (canonical_name, flag_emoji)."""
    return resolve_country(raw)



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⌨️  BUTTON LABELS (CONSTANTS)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Main menu
KB_BUY       = f"{e('buy')} Buy Number"
KB_DEPOSIT   = f"{e('deposit')} Deposit"
KB_PROFILE   = f"{e('profile')} Profile"
KB_HISTORY   = f"{e('history')} History"
KB_SUPPORT   = f"{e('support')} Support"
KB_ADMIN     = f"{e('admin')} Admin Panel"

# Navigation
KB_BACK      = f"{e('back')} Back"
KB_CANCEL    = f"{e('cancel')} Cancel"
KB_CONFIRM   = f"{e('confirm')} Confirm"
KB_HOME      = f"{e('home')} Home"

# Deposit
KB_CRYPTO    = f"{e('crypto')} Crypto (USDT)"
KB_UPI       = f"{e('upi')} UPI (INR)"

# Admin
KB_ADM_ADD     = f"{e('add')} Add Account"
KB_ADM_UPI     = f"{e('card')} Set UPI"
KB_ADM_BAL     = f"{e('money')} Add Balance"
KB_ADM_DEPS    = f"{e('inbox')} Deposit Requests"
KB_ADM_CAST    = f"{e('broadcast')} Broadcast"
KB_ADM_STATS   = f"{e('stats')} Stats"
KB_ADM_ACCS    = f"{e('list')} All Accounts"
KB_ADM_BAN     = "🚫 Ban / Unban"

KB_APPROVE     = f"{e('approve')} Approve"
KB_REJECT      = f"{e('reject')} Reject"
KB_NEXT        = f"{e('next')} Next"
KB_DELETE      = f"{e('delete')} Delete"
KB_EDIT_COUNTRY = f"{e('edit')} Edit Country"

# UPI QR flow
KB_UPLOAD_QR   = f"{e('camera')} Upload QR"
KB_SKIP        = f"{e('next')} Skip"

# All known button labels (for routing)
ALL_BUTTONS = {
    KB_BUY, KB_DEPOSIT, KB_PROFILE, KB_HISTORY, KB_SUPPORT, KB_ADMIN,
    KB_BACK, KB_CANCEL, KB_CONFIRM, KB_HOME,
    KB_CRYPTO, KB_UPI,
    KB_ADM_ADD, KB_ADM_UPI, KB_ADM_BAL, KB_ADM_DEPS,
    KB_ADM_CAST, KB_ADM_STATS, KB_ADM_ACCS, KB_ADM_BAN,
    KB_APPROVE, KB_REJECT, KB_NEXT, KB_DELETE, KB_EDIT_COUNTRY,
    KB_UPLOAD_QR, KB_SKIP,
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⌨️  KEYBOARD BUILDERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def kb_main(uid: int):
    """Main menu keyboard."""
    rows = [
        [Button.text(KB_BUY, resize=True),     Button.text(KB_DEPOSIT, resize=True)],
        [Button.text(KB_PROFILE, resize=True),  Button.text(KB_HISTORY, resize=True)],
        [Button.text(KB_SUPPORT, resize=True)],
    ]
    if uid == ADMIN_ID:
        rows.append([Button.text(KB_ADMIN, resize=True)])
    return rows

def kb_back_home():
    return [[Button.text(KB_BACK, resize=True), Button.text(KB_HOME, resize=True)]]

def kb_back_only():
    return [[Button.text(KB_BACK, resize=True)]]

def kb_cancel_only():
    return [[Button.text(KB_CANCEL, resize=True)]]

def kb_confirm_cancel():
    return [[Button.text(KB_CONFIRM, resize=True), Button.text(KB_CANCEL, resize=True)]]

def kb_deposit_menu():
    return [
        [Button.text(KB_CRYPTO, resize=True), Button.text(KB_UPI, resize=True)],
        [Button.text(KB_BACK, resize=True)],
    ]

def kb_admin_menu():
    return [
        [Button.text(KB_ADM_ADD, resize=True), Button.text(KB_ADM_UPI, resize=True)],
        [Button.text(KB_ADM_BAL, resize=True), Button.text(KB_ADM_DEPS, resize=True)],
        [Button.text(KB_ADM_CAST, resize=True), Button.text(KB_ADM_STATS, resize=True)],
        [Button.text(KB_ADM_ACCS, resize=True), Button.text(KB_ADM_BAN, resize=True)],
        [Button.text(KB_BACK, resize=True)],
    ]

def kb_countries(countries: list[str]):
    """Build a keyboard grid of country buttons + back."""
    rows = []
    row = []
    for c in sorted(countries):
        label = f"{get_flag(c)} {c.title()}"
        row.append(Button.text(label, resize=True))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.text(KB_BACK, resize=True)])
    return rows

def kb_approve_reject_back():
    return [
        [Button.text(KB_APPROVE, resize=True), Button.text(KB_REJECT, resize=True)],
        [Button.text(KB_NEXT, resize=True), Button.text(KB_BACK, resize=True)],
    ]

def kb_delete_back():
    return [
        [Button.text(KB_DELETE, resize=True), Button.text(KB_EDIT_COUNTRY, resize=True)],
        [Button.text(KB_NEXT, resize=True), Button.text(KB_BACK, resize=True)],
    ]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📦  IN-MEMORY STORAGE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

users: dict[int, dict] = {}
user_state: dict[int, dict] = {}
pending_invoices: dict[str, dict] = {}
track_to_order: dict[str, str] = {}
credited_invoices: set[str] = set()
pending_upi: dict[str, dict] = {}
user_active_inr: dict[int, str] = {}
accounts: dict[str, dict] = {}
otp_listeners: dict[str, dict] = {}
purchase_history: dict[int, list] = {}
banned_users: set[int] = set()

admin_settings: dict = {
    "upi_id": UPI_ID,
}

_http: aiohttp.ClientSession | None = None
_purchase_lock = asyncio.Lock()

def http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession()
    return _http

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🛠️  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_user(uid: int, name: str | None = None) -> dict:
    if uid not in users:
        users[uid] = {"balance": 0.0, "joined_at": time.time(), "name": ""}
    if name and name != users[uid].get("name", ""):
        users[uid]["name"] = name
    return users[uid]

def user_mention(uid: int) -> str:
    """Return a clickable HTML user mention. Tapping opens the user's profile."""
    u = users.get(uid, {})
    name = u.get("name", "") or f"User"
    return f'<a href="tg://user?id={uid}">{name}</a> (<code>{uid}</code>)'

def clear_state(uid: int):
    st = user_state.pop(uid, None)
    if st and st.get("data", {}).get("client"):
        try:
            asyncio.create_task(st["data"]["client"].disconnect())
        except Exception:
            pass

def set_state(uid: int, step: str, **kw):
    if uid not in user_state:
        user_state[uid] = {"step": None, "data": {}}
    user_state[uid]["step"] = step
    user_state[uid]["data"].update(kw)

def get_st(uid: int) -> dict | None:
    return user_state.get(uid)

def gen_id(prefix: str = "ACC") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6].upper()}"

def inr_to_usd(amount_inr: float) -> float:
    return round(amount_inr / INR_TO_USD_RATE, 2)

def add_purchase_record(uid: int, acc_id: str, acc: dict):
    if uid not in purchase_history:
        purchase_history[uid] = []
    purchase_history[uid].append({
        "acc_id": acc_id, "phone": acc["phone"], "country": acc["country"],
        "price": acc["price"], "bought_at": time.time(),
    })

def build_header(uid: int) -> str:
    u = get_user(uid)
    stock = sum(1 for a in accounts.values() if a["status"] == "available")
    return (
        f"{e('diamond')} **PREMIUM ACCOUNT MARKET**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{e('user')}  User: `{uid}`\n"
        f"{e('wallet')}  Balance: **${u['balance']:,.2f}**\n"
        f"{e('package')}  Stock: **{stock}** accounts\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{e('bolt')} Instant OTP · {e('lock')} Secure · {e('rocket')} Fast"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💾  PERSISTENCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_data():
    """Sync all in-memory state → MongoDB (primary) → JSON (backup)."""
    try:
        db = get_db()

        # ── Users ──
        db.users.delete_many({})
        if users:
            db.users.insert_many([
                {**data, "user_id": uid} for uid, data in users.items()
            ])

        # ── Accounts (including session_string) ──
        db.accounts.delete_many({})
        if accounts:
            db.accounts.insert_many([
                {**data, "acc_id": aid} for aid, data in accounts.items()
            ])

        # ── Pending UPI ──
        db.pending_upi.delete_many({})
        if pending_upi:
            db.pending_upi.insert_many([
                {**data, "pid": pid} for pid, data in pending_upi.items()
            ])

        # ── Admin Settings ──
        db.settings.delete_many({})
        if admin_settings:
            db.settings.insert_many([
                {"key": k, "value": v} for k, v in admin_settings.items()
            ])

        # ── Credited Invoices ──
        db.credited_invoices.delete_many({})
        if credited_invoices:
            db.credited_invoices.insert_many([
                {"track_id": t} for t in credited_invoices
            ])

        # ── Purchase History ──
        db.purchase_history.delete_many({})
        hist_docs = []
        for uid, records in purchase_history.items():
            for r in records:
                hist_docs.append({**r, "user_id": uid})
        if hist_docs:
            db.purchase_history.insert_many(hist_docs)

        # ── Banned Users ──
        db.banned_users.delete_many({})
        if banned_users:
            db.banned_users.insert_many([
                {"user_id": uid} for uid in banned_users
            ])

        # ── JSON backup ──
        backup_all()

    except Exception as exc:
        log.error(f"MongoDB save failed: {exc}")
        # Fallback: save to legacy JSON file
        try:
            d = {
                "users": {str(k): v for k, v in users.items()},
                "accounts": {k: {kk: vv for kk, vv in v.items() if kk != "session_string"}
                             for k, v in accounts.items()},
                "accounts_sessions": {k: v.get("session_string", "") for k, v in accounts.items()},
                "pending_upi": pending_upi,
                "settings": admin_settings,
                "credited_invoices": list(credited_invoices),
                "purchase_history": {str(k): v for k, v in purchase_history.items()},
                "banned_users": list(banned_users),
            }
            DATA_FILE.write_text(json.dumps(d, indent=2, default=str))
            log.info("Fallback: saved to JSON (MongoDB was unavailable)")
        except Exception as exc2:
            log.error(f"JSON fallback also failed: {exc2}")


def load_data():
    """Load all data from MongoDB into memory. Recover from JSON if Mongo empty."""
    # Step 1: Check MongoDB connection and recover if needed
    try:
        check_and_recover()
    except ConnectionError:
        log.critical("MongoDB unreachable — attempting legacy JSON load")
        _load_legacy_json()
        return
    except Exception as exc:
        log.error(f"Recovery check failed: {exc}")

    # Step 2: Load all collections from MongoDB into in-memory dicts
    try:
        db = get_db()

        # ── Users ──
        for doc in db.users.find({}, {"_id": 0}):
            uid = doc.pop("user_id", None)
            if uid is not None:
                users[int(uid)] = doc

        # ── Accounts ──
        for doc in db.accounts.find({}, {"_id": 0}):
            aid = doc.pop("acc_id", None)
            if aid:
                accounts[aid] = doc

        # ── Pending UPI ──
        for doc in db.pending_upi.find({}, {"_id": 0}):
            pid = doc.pop("pid", None)
            if pid:
                pending_upi[pid] = doc

        # ── Admin Settings ──
        for doc in db.settings.find({}, {"_id": 0}):
            key = doc.get("key")
            if key:
                admin_settings[key] = doc["value"]

        # ── Credited Invoices ──
        for doc in db.credited_invoices.find({}, {"_id": 0}):
            tid = doc.get("track_id")
            if tid:
                credited_invoices.add(tid)

        # ── Purchase History ──
        for doc in db.purchase_history.find({}, {"_id": 0}):
            uid = doc.pop("user_id", None)
            if uid is not None:
                uid = int(uid)
                if uid not in purchase_history:
                    purchase_history[uid] = []
                purchase_history[uid].append(doc)

        # ── Banned Users ──
        for doc in db.banned_users.find({}, {"_id": 0}):
            uid = doc.get("user_id")
            if uid is not None:
                banned_users.add(int(uid))

        log.info(f"Loaded from MongoDB: {len(users)} users, {len(accounts)} accounts")

    except Exception as exc:
        log.error(f"MongoDB load failed: {exc}")
        _load_legacy_json()


def _load_legacy_json():
    """Fallback: load from bot_store_data.json if MongoDB is unavailable."""
    if not DATA_FILE.exists():
        log.warning("No legacy JSON file found either — starting fresh")
        return
    try:
        d = json.loads(DATA_FILE.read_text())
        for k, v in d.get("users", {}).items():
            users[int(k)] = v
        sessions_map = d.get("accounts_sessions", {})
        for k, v in d.get("accounts", {}).items():
            v["session_string"] = sessions_map.get(k, "")
            accounts[k] = v
        pending_upi.update(d.get("pending_upi", {}))
        admin_settings.update(d.get("settings", {}))
        credited_invoices.update(d.get("credited_invoices", []))
        banned_users.update(int(x) for x in d.get("banned_users", []))
        for k, v in d.get("purchase_history", {}).items():
            purchase_history[int(k)] = v
        log.info(f"Loaded from legacy JSON: {len(users)} users, {len(accounts)} accounts")
    except Exception as exc:
        log.error(f"Legacy JSON load failed: {exc}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🌐  OXAPAY API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def create_invoice(amount: float, coin: str, order_id: str) -> dict | None:
    url = f"{OXAPAY_API_BASE}/payment/invoice"
    headers = {"merchant_api_key": OXAPAY_MERCHANT_KEY, "Content-Type": "application/json"}
    payload = {
        "amount": amount, "currency": coin, "lifetime": INVOICE_LIFETIME_MINUTES,
        "fee_paid_by_payer": 1, "under_paid_coverage": 1,
        "order_id": order_id, "description": f"Deposit {amount} {coin}", "sandbox": False,
    }
    for attempt in range(1, 3):
        try:
            async with http().post(url, json=payload, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json(content_type=None)
                if data.get("status") != 200:
                    return None
                inv = data.get("data") or {}
                pu, ti = inv.get("payment_url"), inv.get("track_id")
                if pu and ti:
                    return {"payment_url": str(pu), "track_id": str(ti)}
                return None
        except asyncio.TimeoutError:
            pass
        except Exception as exc:
            log.error(f"OxaPay error: {exc}")
            return None
        if attempt < 2:
            await asyncio.sleep(1.5)
    return None

async def inquiry_invoice(track_id: str) -> dict | None:
    url = "https://api.oxapay.com/merchants/inquiry"
    payload = {"merchant": OXAPAY_MERCHANT_KEY, "trackId": int(track_id)}
    try:
        async with http().post(url, json=payload, headers={"Content-Type": "application/json"},
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json(content_type=None)
            return data if data.get("result") == 100 else None
    except Exception:
        return None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📩  AUTO OTP LISTENER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def start_otp_listener(bot_client, acc_id: str, buyer_uid: int):
    acc = accounts.get(acc_id)
    if not acc:
        return
    phone = acc["phone"]
    _otp_done = asyncio.Event()          # bulletproof single-fire flag

    try:
        client = TelegramClient(StringSession(acc["session_string"]), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            log.warning(f"Session expired: {acc_id}")
            acc["status"] = "available"
            acc["buyer"] = None
            acc["bought_at"] = None
            save_data()
            try:
                await bot_client.send_message(buyer_uid,
                    "⚠️ **Session Failed**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📱 `{phone}` — session expired.\n"
                    "💰 You were NOT charged.\n\n"
                    "💡 Try another account.",
                    buttons=kb_main(buyer_uid), parse_mode="md")
            except Exception:
                pass
            await client.disconnect()
            return

        @client.on(events.NewMessage(from_users=777000))
        async def otp_handler(event):
            # ── Duplicate guard: only fire ONCE ──
            if _otp_done.is_set():
                return
            codes = re.findall(r'\b(\d{5,6})\b', event.raw_text or "")
            if not codes:
                return
            otp = codes[0]
            _otp_done.set()                # lock immediately before any await

            # ── 1) SEND OTP TO BUYER INSTANTLY ──
            try:
                await send_premium(bot_client, buyer_uid,
                    f"{e('mail')} **OTP RECEIVED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{e('phone')}  `{phone}`\n"
                    f"{e('two_fa')}  OTP: `{otp}`\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{e('bolt')}  Use this code quickly!",
                    buttons=kb_main(buyer_uid), parse_mode="md")
                log.info(f"OTP -> {buyer_uid}: {otp}")
            except Exception as exc:
                log.warning(f"OTP send failed: {exc}")

            # ── 2) Charge user (after OTP is already delivered) ──
            u = get_user(buyer_uid)
            price = acc["price"]

            if u["balance"] < price:
                acc["status"] = "available"
                acc["buyer"] = None
                acc["bought_at"] = None
                save_data()
                try:
                    await bot_client.send_message(buyer_uid,
                        "❌ **Purchase Failed**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "💰 Insufficient balance when OTP arrived.\n"
                        "Deposit more and try again.",
                        buttons=kb_main(buyer_uid), parse_mode="md")
                except Exception:
                    pass
                await client.disconnect()
                return

            u["balance"] -= price
            acc["status"] = "reserved"
            acc["bought_at"] = time.time()
            add_purchase_record(buyer_uid, acc_id, acc)
            save_data()
            log.info(f"Charged: {acc_id} -> {buyer_uid} ${price}")

            # ── 3) Notify admin (background, non-blocking for buyer) ──
            try:
                f = get_flag(acc["country"])
                await send_premium(bot_client, ADMIN_ID,
                    f"{e('cart')} **ACCOUNT SOLD**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{e('user')} `{buyer_uid}` • {e('phone')} `{phone}` • ${price:.2f}\n"
                    f"{e('globe')} {f} {acc['country'].title()} • {e('id')} `{acc_id}`",
                    parse_mode="md")
            except Exception:
                pass

            log.info(f"Disconnecting OTP listener: {acc_id}")
            await client.disconnect()

        otp_listeners[acc_id] = {"client": client, "buyer_uid": buyer_uid, "started_at": time.time()}
        log.info(f"OTP listener started: {acc_id} -> {buyer_uid}")
        await client.run_until_disconnected()

    except UserDeactivatedBanError:
        log.warning(f"Account banned: {acc_id}")
        if acc["status"] == "pending_otp":
            acc["status"] = "available"
            acc["buyer"] = None
            acc["bought_at"] = None
            save_data()
            try:
                await bot_client.send_message(buyer_uid,
                    "🚫 **Account Banned**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📱 `{phone}` is deactivated.\n💰 You were NOT charged.",
                    buttons=kb_main(buyer_uid), parse_mode="md")
            except Exception:
                pass
    except Exception as exc:
        log.error(f"OTP error {acc_id}: {exc}")
        if acc["status"] == "pending_otp":
            acc["status"] = "available"
            acc["buyer"] = None
            acc["bought_at"] = None
            save_data()
            try:
                await bot_client.send_message(buyer_uid,
                    "⚠️ **Session Error**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📱 `{phone}`\n💰 You were NOT charged.\n\n"
                    "💡 Try another account.",
                    buttons=kb_main(buyer_uid), parse_mode="md")
            except Exception:
                pass
    finally:
        otp_listeners.pop(acc_id, None)

async def stop_otp_listener(acc_id: str):
    listener = otp_listeners.pop(acc_id, None)
    if listener and listener.get("client"):
        try:
            await listener["client"].disconnect()
        except Exception:
            pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔄  BACKGROUND TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def poll_invoices():
    log.info(f"Crypto poll started ({POLL_INTERVAL_SECONDS}s)")
    while True:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        for order_id, inv in list(pending_invoices.items()):
            if inv.get("credited"):
                continue
            age = (time.time() - inv["created_at"]) / 60
            if age > INVOICE_LIFETIME_MINUTES + 2:
                if inv.get("status") not in ("expired", "failed"):
                    inv["status"] = "expired"
                    uid = inv["user_id"]
                    clear_state(uid)
                    pending_invoices.pop(order_id, None)
                    track_to_order.pop(inv["track_id"], None)
                    try:
                        await send_premium(bot, uid,
                            "⏰ **Invoice Expired**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            "❌ Payment not received in time.\n💡 Create a new one.",
                            buttons=kb_main(uid), parse_mode="md")
                    except Exception:
                        pass
                continue

            result = await inquiry_invoice(inv["track_id"])
            if not result:
                continue
            status = str(result.get("status") or "").lower()
            actual_paid = float(result.get("payAmount") or 0)
            uid = inv["user_id"]
            u = get_user(uid)
            prev = inv.get("status", "")

            if status in ("paying", "waiting", "confirming") and prev != status:
                inv["status"] = status
                display = actual_paid if actual_paid > 0 else inv["amount"]
                try:
                    await send_premium(bot, uid,
                        "⏳ **Payment Confirming**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🪙 USDT • `${display:.2f}`\n\n"
                        "📡 Transaction detected!\n⌛ Waiting for confirmation…",
                        parse_mode="md")
                except Exception:
                    pass

            elif status == "paid" and not inv.get("credited"):
                track_id = inv["track_id"]
                if track_id in credited_invoices:
                    log.warning(f"Double credit blocked: {track_id}")
                    inv["credited"] = True
                    pending_invoices.pop(order_id, None)
                    track_to_order.pop(track_id, None)
                    continue

                credited = actual_paid if actual_paid > 0 else 0
                if credited < MIN_CREDIT_AMOUNT:
                    log.warning(f"Spam payment ignored: ${credited:.4f}")
                    inv["credited"] = True
                    inv["status"] = "spam_ignored"
                    pending_invoices.pop(order_id, None)
                    track_to_order.pop(track_id, None)
                    try:
                        await send_premium(bot, uid,
                            "⚠️ **Payment Too Small**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"💰 Received: `${credited:.4f}`\n"
                            f"📌 Minimum: `${MIN_CREDIT_AMOUNT:.2f}`\n\n"
                            "❌ Amount not credited.",
                            buttons=kb_main(uid), parse_mode="md")
                    except Exception:
                        pass
                    continue

                inv["credited"] = True
                inv["status"] = "paid"
                credited_invoices.add(track_id)
                u["balance"] += credited
                clear_state(uid)
                pending_invoices.pop(order_id, None)
                track_to_order.pop(track_id, None)
                save_data()
                try:
                    await send_premium(bot, uid,
                        "✅ **Payment Successful!**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Added: `${credited:.2f} USDT`\n"
                        f"💼 Balance: `${u['balance']:,.2f}`\n\n"
                        "⚡ Verified · Secure",
                        buttons=kb_main(uid), parse_mode="md")
                except Exception:
                    pass
                log.info(f"Crypto paid: {uid} +${credited} (requested=${inv['amount']}, actual=${credited})")

            elif status in ("expired", "failed", "refunded") and prev != status:
                inv["status"] = status
                clear_state(uid)
                pending_invoices.pop(order_id, None)
                track_to_order.pop(inv["track_id"], None)
                try:
                    await send_premium(bot, uid,
                        "⏰ **Invoice Expired**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "❌ Payment not completed.",
                        buttons=kb_main(uid), parse_mode="md")
                except Exception:
                    pass


async def poll_inr_expiry():
    log.info(f"INR expiry task started")
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for pid, p in list(pending_upi.items()):
            if p["status"] != "pending":
                continue
            if (now - p["created_at"]) > INR_PAYMENT_EXPIRY:
                p["status"] = "expired"
                uid = p["user_id"]
                user_active_inr.pop(uid, None)
                clear_state(uid)
                try:
                    await send_premium(bot, uid,
                        "⏰ **INR Payment Expired**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🆔 `{pid}` • ₹{p['amount']:,.0f}\n\n"
                        "Create a new deposit.",
                        buttons=kb_main(uid), parse_mode="md")
                except Exception:
                    pass
                log.info(f"INR expired: {pid}")


async def session_expiry_task():
    log.info(f"Session expiry task ({SESSION_EXPIRY_HOURS}h)")
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for acc_id, acc in list(accounts.items()):
            if acc["status"] == "pending_otp" and acc.get("bought_at"):
                if (now - acc["bought_at"]) / 60 > 10:
                    buyer = acc.get("buyer")
                    await stop_otp_listener(acc_id)
                    acc["status"] = "available"
                    acc["buyer"] = None
                    acc["bought_at"] = None
                    save_data()
                    if buyer:
                        try:
                            await send_premium(bot, buyer,
                                "⏰ **OTP TIMEOUT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"📱 `{acc['phone']}` — no OTP received.\n"
                                "💰 You were NOT charged.\n\n"
                                "💡 Try again or pick another.",
                                buttons=kb_main(buyer), parse_mode="md")
                        except Exception:
                            pass
                    log.info(f"OTP timeout: {acc_id}")
                continue

            if acc["status"] != "reserved" or not acc.get("bought_at"):
                continue
            if (now - acc["bought_at"]) / 3600 < SESSION_EXPIRY_HOURS:
                continue

            buyer = acc.get("buyer")
            await stop_otp_listener(acc_id)
            try:
                temp = TelegramClient(StringSession(acc["session_string"]), API_ID, API_HASH)
                await temp.connect()
                if await temp.is_user_authorized():
                    await temp.log_out()
                await temp.disconnect()
            except Exception:
                pass

            acc["status"] = "sold"
            save_data()
            if buyer:
                try:
                    await send_premium(bot, buyer,
                        "⏰ **SESSION EXPIRED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"📱 `{acc['phone']}` — logged out.\n\n"
                        "💡 Purchase a new one!",
                        buttons=kb_main(buyer), parse_mode="md")
                except Exception:
                    pass
            log.info(f"Session expired: {acc_id}")


async def autosave_task():
    """Periodic sync: in-memory → MongoDB → backup.json"""
    while True:
        await asyncio.sleep(300)
        save_data()
        log.debug("Auto-save: MongoDB + backup.json synced")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🤖  BOT CLIENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

bot = TelegramClient("premium_bot_session", API_ID, API_HASH)


async def safe_edit(event, text, buttons=None, parse_mode="md"):
    """Edit an inline message safely, with premium emoji overlay."""
    try:
        await safe_edit_premium(event, text, buttons=buttons, parse_mode=parse_mode)
    except MessageNotModifiedError:
        pass
    except Exception as ex:
        log.warning(f"Edit failed: {ex}")


async def send_menu(uid: int, chat_id: int):
    """Send main menu with header + reply keyboard."""
    header = build_header(uid)
    await send_premium(bot, chat_id, header, buttons=kb_main(uid), parse_mode="md")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📝  MASTER MESSAGE HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.on(events.NewMessage(incoming=True))
async def master_handler(event):
    uid = event.sender_id
    text = (event.text or "").strip()
    sender = await event.get_sender()
    sender_name = getattr(sender, "first_name", "") or ""
    u = get_user(uid, name=sender_name)
    st = get_st(uid)
    step = st["step"] if st else None

    # ══════════════════════════════════════
    #  BAN CHECK
    # ══════════════════════════════════════
    if uid in banned_users and uid != ADMIN_ID:
        await respond_premium(event,
            "🚫 **You are banned.**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Contact support if you believe this is a mistake.",
            parse_mode="md")
        return

    # ══════════════════════════════════════
    #  COMMANDS: /start, /cancel
    # ══════════════════════════════════════
    if text == "/start":
        clear_state(uid)
        await send_menu(uid, event.chat_id)
        return

    if text == "/cancel" or text == KB_CANCEL:
        clear_state(uid)
        await respond_premium(event, 
            "❌ **Action Cancelled**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have exited the current process.",
            buttons=kb_main(uid), parse_mode="md")
        return

    # ══════════════════════════════════════
    #  GLOBAL: Home / Back (when no step)
    # ══════════════════════════════════════
    if text == KB_HOME:
        clear_state(uid)
        await send_menu(uid, event.chat_id)
        return

    if text == KB_BACK and not step:
        clear_state(uid)
        await send_menu(uid, event.chat_id)
        return

    # ══════════════════════════════════════
    #  MAIN MENU BUTTONS (always work, even mid-flow)
    # ══════════════════════════════════════

    # ── 🛒 Buy Number ──
    if text == KB_BUY:
        clear_state(uid)
        await handle_buy_menu(event, uid, u)
        return

    # ── 💰 Deposit Money ──
    if text == KB_DEPOSIT:
        clear_state(uid)
        set_state(uid, "deposit_menu")
        await respond_premium(event, 
            f"{e('deposit')} **DEPOSIT FUNDS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Select deposit method:\n\n"
            f"{e('crypto')}  **Crypto** — USDT auto-verify\n"
            f"{e('upi')}  **UPI** — INR manual transfer\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            buttons=[
                [Button.inline(f"{e('usdt')} Crypto (USDT)", b"dep_crypto"), Button.inline(f"{e('rupee')} UPI (INR)", b"dep_upi")],
                [Button.inline(f"{e('back')} Back", b"go_home")],
            ], parse_mode="md")
        return

    # ── 👤 Profile ──
    if text == KB_PROFILE:
        clear_state(uid)
        bought = sum(1 for a in accounts.values() if a.get("buyer") == uid)
        active = sum(1 for a in accounts.values() if a.get("buyer") == uid and a["status"] == "reserved")
        joined = datetime.fromtimestamp(u.get("joined_at", time.time())).strftime("%d %b %Y")
        await respond_premium(event,
            f"{e('user')} **YOUR PROFILE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{e('id')}  `{uid}`\n"
            f"{e('wallet')}  Balance: **${u['balance']:,.2f}**\n"
            f"{e('cart')}  Purchased: **{bought}**\n"
            f"{e('phone')}  Active: **{active}**\n"
            f"{e('clock')}  Joined: {joined}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━",
            buttons=kb_main(uid), parse_mode="md")
        return

    # ── 📦 Purchased History ──
    if text == KB_HISTORY:
        clear_state(uid)
        await handle_history(event, uid)
        return

    # ── 🆘 Support ──
    if text == KB_SUPPORT:
        clear_state(uid)
        
        btns = []
        # WARNING: Telegram specifically disables `tg://user?id=...` deep links in inline buttons!
        # If you set SUPPORT_LINK to tg://user..., the button WILL show up but clicking it will do nothing.
        # To make the button clickable, you MUST use a username link like: https://t.me/YourUsername
        btns.append([Button.url(f"{e('msg')} Contact Admin", SUPPORT_LINK)])
        btns.append([Button.inline(f"{e('back')} Back to Menu", b"go_home")])

        await respond_premium(event,
            f"{e('support')} **SUPPORT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{e('crown')} Admin: [{ADMIN_ID}](tg://user?id={ADMIN_ID})\n\n"
            f"{e('mail')} Contact for:\n  {e('cart')} Account issues\n  {e('deposit')} Deposit issues\n  {e('key')} OTP problems\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            buttons=btns, parse_mode="md")
        return

    # ── 👑 Admin Panel ──
    if text == KB_ADMIN and uid == ADMIN_ID:
        clear_state(uid)
        await handle_admin_menu(event, uid)
        return

    # ══════════════════════════════════════
    #  STEP-BASED ROUTING (State Machine)
    # ══════════════════════════════════════

    if not step:
        return  # No active flow, ignore unknown text

    # ── Back in any step → return to appropriate parent ──
    if text == KB_BACK:
        return await handle_back(event, uid, step)

    # ── Deposit Menu ──
    if step == "deposit_menu":
        await handle_deposit_choice(event, uid, text)
        return

    # ── Crypto amount ──
    if step == "crypto_amount":
        await handle_crypto_amount(event, uid, text)
        return

    # ── UPI amount ──
    if step == "upi_amount":
        await handle_upi_amount(event, uid, text)
        return

    # ── UPI proof ──
    if step == "upi_proof":
        await handle_upi_proof(event, uid, text)
        return

    # ── Buy: country selection ──
    if step == "buy_country":
        await handle_buy_country(event, uid, u, text)
        return

    # ── Buy: confirm ──
    if step == "buy_confirm":
        await handle_buy_confirm(event, uid, u, text)
        return

    # ── Admin flows ──
    if step == "admin_menu":
        await handle_admin_choice(event, uid, text)
        return

    if step == "adm_phone":
        await handle_adm_phone(event, uid, text)
        return

    if step == "adm_otp":
        await handle_adm_otp(event, uid, text)
        return

    if step == "adm_2fa":
        await handle_adm_2fa(event, uid, text)
        return

    if step == "adm_country":
        await handle_adm_country(event, uid, text)
        return

    if step == "adm_price":
        await handle_adm_price(event, uid, text)
        return

    if step == "adm_set_upi":
        await handle_adm_set_upi(event, uid, text)
        return

    if step == "adm_upi_qr":
        await handle_adm_upi_qr(event, uid, text)
        return

    if step == "adm_bal_uid":
        await handle_adm_bal_uid(event, uid, text)
        return

    if step == "adm_bal_amount":
        await handle_adm_bal_amount(event, uid, text)
        return

    if step == "adm_broadcast":
        await handle_adm_broadcast(event, uid, text)
        return

    if step == "adm_deps_review":
        await handle_adm_deps_review(event, uid, text)
        return

    if step == "adm_accs_review":
        await handle_adm_accs_review(event, uid, text)
        return

    if step == "adm_edit_country":
        await handle_adm_edit_country(event, uid, text)
        return

    if step == "adm_ban_uid":
        await handle_adm_ban_uid(event, uid, text)
        return


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔙  BACK HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def handle_back(event, uid, step):
    """Route back button to the correct parent screen."""
    # Flows that go back to main menu
    if step in ("deposit_menu", "buy_country", "admin_menu"):
        clear_state(uid)
        await send_menu(uid, event.chat_id)
        return

    # Flows that go back to deposit menu
    if step in ("crypto_amount", "upi_amount", "upi_proof"):
        clear_state(uid)
        set_state(uid, "deposit_menu")
        await respond_premium(event, 
            f"{e('deposit')} **DEPOSIT FUNDS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Select deposit method:\n\n"
            f"{e('usdt')}  **Crypto** — USDT auto-verify\n"
            f"{e('upi')}  **UPI** — INR manual transfer\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            buttons=kb_deposit_menu(), parse_mode="md")
        return

    # Flows that go back to buy country
    if step == "buy_confirm":
        clear_state(uid)
        await handle_buy_menu(event, uid, get_user(uid))
        return

    # Admin sub-flows go back to admin menu
    if step.startswith("adm_"):
        clear_state(uid)
        await handle_admin_menu(event, uid)
        return

    # Default: main menu
    clear_state(uid)
    await send_menu(uid, event.chat_id)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🛒  BUY FLOW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def handle_buy_menu(event, uid, u, edit=False):
    """Show country list for buying."""
    clear_state(uid)
    country_data: dict[str, dict] = {}
    for a in accounts.values():
        if a["status"] != "available":
            continue
        c = a["country"]
        if c not in country_data:
            country_data[c] = {"count": 0, "price": a["price"]}
        country_data[c]["count"] += 1
        country_data[c]["price"] = min(country_data[c]["price"], a["price"])

    text = f"{e('cart')} **BUY TELEGRAM ACCOUNT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    if not country_data:
        text += f"{e('cross')} No accounts available right now.\n\n{e('bulb')} Check back later!"
        if edit:
            await safe_edit(event, text, buttons=[[Button.inline(f"{e('back')} Back", b"go_home")]])
        else:
            await respond_premium(event, text, buttons=kb_main(uid), parse_mode="md")
        return

    text += f"{e('globe')} Select a country:\n\n"
    for c in sorted(country_data):
        d = country_data[c]
        text += f"{get_flag(c)}  **{c.title()}** • {e('dollar')}${d['price']:.2f} • {e('package')} Stock: {d['count']}\n"
    text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n{e('arrow')} Tap a country:"

    # Build inline country buttons
    rows = []
    row = []
    for c in sorted(country_data):
        label = f"{get_flag(c)} {c.title()}"
        row.append(Button.inline(label, f"buy_{c}".encode()))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.inline(f"{e('back')} Back", b"go_home")])

    if edit:
        await safe_edit(event, text, buttons=rows)
    else:
        await respond_premium(event, text, buttons=rows, parse_mode="md")


async def handle_buy_country(event, uid, u, text):
    """User selected a country from keyboard."""
    st = get_st(uid)
    available = st["data"].get("available_countries", [])

    # Parse country from button text like "🇮🇳 India"
    # Strip flag emoji (first 2+ chars) and get the name
    selected = None
    for c in available:
        label = f"{get_flag(c)} {c.title()}"
        if text.strip() == label.strip():
            selected = c
            break

    if not selected:
        # Try matching just the name
        for c in available:
            if text.lower().strip() == c.lower():
                selected = c
                break

    if not selected:
        await respond_premium(event, 
            "⚠️ Invalid selection. Tap one of the country buttons below.",
            buttons=kb_countries(available), parse_mode="md")
        return

    # Find cheapest account for this country
    avail = [(aid, a) for aid, a in accounts.items()
             if a["status"] == "available" and a["country"] == selected]
    if not avail:
        await respond_premium(event, "❌ Out of stock! Try another.", buttons=kb_countries(available), parse_mode="md")
        return

    avail.sort(key=lambda x: x[1]["price"])
    acc_id, acc = avail[0]
    f = get_flag(selected)

    text = (
        f"{e('cart')} **CONFIRM PURCHASE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{f}  **Country:** {selected.title()}\n"
        f"{e('phone')}  **Phone:** `{acc['phone']}`\n"
        f"{e('dollar')}  **Price:** ${acc['price']:.2f}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{e('briefcase')}  Your Balance: ${u['balance']:,.2f}\n"
    )

    if u["balance"] < acc["price"]:
        text += f"\n{e('warn')} **Insufficient balance!**\n{e('bulb')} Deposit funds first."
        await respond_premium(event, text, buttons=kb_main(uid), parse_mode="md")
        clear_state(uid)
        return

    text += f"{e('down')}  After: ${u['balance'] - acc['price']:.2f}\n\n"
    text += f"{e('bolt')} OTP auto-delivered after purchase."

    set_state(uid, "buy_confirm", acc_id=acc_id)
    confirm_btns = [
        [Button.inline(f"{e('confirm')} Confirm Purchase", f"confirm_{acc_id}".encode()),
         Button.inline(f"{e('cancel')} Cancel", b"go_home")],
    ]
    await respond_premium(event, text, buttons=confirm_btns, parse_mode="md")


async def handle_buy_confirm(event, uid, u, text):
    """User confirmed or cancelled purchase."""
    st = get_st(uid)
    acc_id = st["data"].get("acc_id")

    if text != KB_CONFIRM:
        clear_state(uid)
        await send_menu(uid, event.chat_id)
        return

    acc = accounts.get(acc_id)
    if not acc or acc["status"] != "available":
        clear_state(uid)
        await respond_premium(event, "⚠️ Account no longer available!",
                            buttons=kb_main(uid), parse_mode="md")
        return

    if u["balance"] < acc["price"]:
        clear_state(uid)
        await respond_premium(event, "❌ Insufficient balance!", buttons=kb_main(uid), parse_mode="md")
        return

    async with _purchase_lock:
        if acc["status"] != "available" or u["balance"] < acc["price"]:
            clear_state(uid)
            await respond_premium(event, "⚠️ Failed — try again.", buttons=kb_main(uid), parse_mode="md")
            return
        acc["status"] = "pending_otp"
        acc["buyer"] = uid
        acc["bought_at"] = time.time()
        save_data()

    phone = acc["phone"]
    f = get_flag(acc["country"])
    clear_state(uid)

    await respond_premium(event, 
        "⏳ **CONNECTING TO ACCOUNT…**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{f}  {acc['country'].title()}\n"
        f"📱  `{phone}`\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Waiting for OTP delivery…\n"
        "📩 Code will appear here automatically.\n\n"
        "💰 **Balance will be charged only\n"
        "    after OTP is confirmed.**\n\n"
        f"⏱  Valid for **{SESSION_EXPIRY_HOURS} hours** after OTP\n"
        "⚠️ Do NOT share this account.\n━━━━━━━━━━━━━━━━━━━━━━━━━━",
        buttons=kb_main(uid), parse_mode="md")

    asyncio.create_task(start_otp_listener(bot, acc_id, uid))
    log.info(f"Locked for OTP: {acc_id} -> {uid} ${acc['price']}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📦  HISTORY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def handle_history(event, uid):
    records = purchase_history.get(uid, [])
    account_purchases = [
        {"acc_id": aid, "phone": a["phone"], "country": a["country"],
         "price": a["price"], "bought_at": a.get("bought_at", 0)}
        for aid, a in accounts.items()
        if a.get("buyer") == uid and a["status"] in ("reserved", "sold")
    ]
    seen = {r["acc_id"] for r in records}
    for ap in account_purchases:
        if ap["acc_id"] not in seen:
            records.append(ap)

    text = "📦 **PURCHASED HISTORY**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    if not records:
        text += "📭 No purchases yet.\n\n💡 Buy your first account!"
        await respond_premium(event, text, buttons=kb_main(uid), parse_mode="md")
        return

    records.sort(key=lambda x: x.get("bought_at", 0), reverse=True)
    for r in records[:10]:
        ts = datetime.fromtimestamp(r.get("bought_at", 0)).strftime("%d %b %Y %H:%M") if r.get("bought_at") else "N/A"
        f = get_flag(r.get("country", ""))
        status_icon = "✅"
        acc = accounts.get(r["acc_id"])
        if acc:
            if acc["status"] == "reserved": status_icon = "🟢"
            elif acc["status"] == "sold": status_icon = "🔒"
            elif acc["status"] == "available": status_icon = "🔄"
        text += (
            f"{status_icon} `{r['acc_id']}`\n"
            f"   {f} {r.get('country', '?').title()} • `{r['phone']}` • ${r['price']:.2f}\n"
            f"   📅 {ts}\n\n"
        )
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n🟢 Active  🔒 Expired  🔄 Released"
    await respond_premium(event, text, buttons=kb_main(uid), parse_mode="md")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💰  DEPOSIT FLOW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def handle_deposit_choice(event, uid, text):
    if text == KB_CRYPTO:
        set_state(uid, "crypto_amount")
        await respond_premium(event, 
            "💵 **ENTER AMOUNT — USDT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Type amount of **USDT** to deposit.\n\n"
            "📌 Examples: `10` · `50` · `100`\n\n⬇️ Send amount:",
            buttons=kb_back_only(), parse_mode="md")
        return

    if text == KB_UPI:
        pid = user_active_inr.get(uid)
        if pid and pid in pending_upi and pending_upi[pid]["status"] == "pending":
            p = pending_upi[pid]
            await respond_premium(event, 
                "⚠️ **PENDING PAYMENT EXISTS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 `{pid}` • ₹{p['amount']:,.0f}\n\nComplete or wait for approval.",
                buttons=kb_main(uid), parse_mode="md")
            clear_state(uid)
            return

        set_state(uid, "upi_amount")
        await respond_premium(event, 
            f"{e('upi')} **UPI DEPOSIT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{e('pin')} Min: ₹{INR_MIN_AMOUNT} · Max: ₹{INR_MAX_AMOUNT:,}\n\n"
            f"{e('arrow')} Enter amount in ₹:",
            buttons=kb_back_only(), parse_mode="md")
        return

    await respond_premium(event, "⚠️ Tap one of the buttons below.", buttons=kb_deposit_menu(), parse_mode="md")


async def handle_crypto_amount(event, uid, text):
    try:
        amount = float(text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except (ValueError, AttributeError):
        await respond_premium(event, "⚠️ Invalid amount. Example: `10` or `50.5`\n\n⬇️ Try again:",
                            buttons=kb_back_only(), parse_mode="md")
        return

    order_id = f"UID{uid}-{uuid.uuid4().hex[:8].upper()}"
    await respond_premium(event, "⏳ **Creating Invoice…**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⌛ Please wait…",
                        parse_mode="md")

    result = await create_invoice(amount, "USDT", order_id)
    if not result:
        clear_state(uid)
        await respond_premium(event, "❌ **Failed to create invoice.**\n\n💡 Try again.",
                            buttons=kb_main(uid), parse_mode="md")
        return

    payment_url, track_id = result["payment_url"], result["track_id"]
    pending_invoices[order_id] = {
        "user_id": uid, "coin": "USDT", "amount": amount,
        "track_id": track_id, "created_at": time.time(),
        "credited": False, "status": "waiting",
    }
    track_to_order[track_id] = order_id
    clear_state(uid)  # Polling handles payment independently; free the user

    await respond_premium(event, 
        f"{e('sent')} **PAYMENT INVOICE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{e('usdt')} **Coin:** USDT\n"
        f"{e('dollar')} **Amount:** `${amount:.2f}`\n"
        f"{e('timer')} **Expires:** {INVOICE_LIFETIME_MINUTES} min\n"
        f"{e('tag')} **Track:** `{track_id}`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ Tap **Pay Now** below\n"
        f"2️⃣ Send exact amount\n"
        f"3️⃣ Auto-verified! {e('check')}\n\n"
        f"{e('hourglass')} Watching for payment…",
        buttons=[
            [Button.url(f"{e('link')} Pay Now", payment_url)],
            [Button.inline(f"{e('back')} Back to Menu", b"go_home")],
        ], parse_mode="md")
    log.info(f"Invoice: {uid} ${amount} USDT track={track_id}")


async def handle_upi_amount(event, uid, text):
    try:
        amount = float(text.replace(",", "").replace("₹", ""))
        if amount < INR_MIN_AMOUNT:
            raise ValueError
        if amount > INR_MAX_AMOUNT:
            raise ValueError
    except (ValueError, AttributeError):
        await respond_premium(event, 
            f"⚠️ Invalid. Range: ₹{INR_MIN_AMOUNT} — ₹{INR_MAX_AMOUNT:,}\n\n⬇️ Try again:",
            buttons=kb_back_only(), parse_mode="md")
        return

    usd = inr_to_usd(amount)
    set_state(uid, "upi_proof", amount=amount)

    upi_id = admin_settings['upi_id']
    caption = (
        f"{e('upi')} **UPI DEPOSIT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{e('money')} Amount: **₹{amount:,.0f}** (~${usd:.2f})\n"
        f"{e('upi')} UPI: `{upi_id}`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "1️⃣ Send **exact** amount to UPI\n"
        "2️⃣ Send **UTR number** or **screenshot** here\n\n"
        f"{e('timer')} Expires: {INR_PAYMENT_EXPIRY // 60} min\n\n"
        f"{e('check')} Send proof now:"
    )

    qr_exists = admin_settings.get("upi_qr") and UPI_QR_FILE.exists()
    if qr_exists:
        try:
            await bot.send_file(
                event.chat_id, str(UPI_QR_FILE),
                caption=caption,
                buttons=kb_cancel_only(),
                parse_mode="md"
            )
        except Exception as ex:
            log.warning(f"QR send failed: {ex}")
            await respond_premium(event, caption,
                buttons=kb_cancel_only(), parse_mode="md")
    else:
        await respond_premium(event, caption,
            buttons=kb_cancel_only(), parse_mode="md")


async def handle_upi_proof(event, uid, text):
    st = get_st(uid)
    proof = text or None
    photo = event.photo
    if not proof and not photo:
        await respond_premium(event, "⚠️ Send UTR number or screenshot.", buttons=kb_cancel_only(), parse_mode="md")
        return

    amount = st["data"].get("amount", 0)
    pid = gen_id("INR")
    proof_text = proof or "📸 Screenshot"

    pending_upi[pid] = {
        "user_id": uid, "amount": amount, "method": "upi",
        "proof": proof_text, "status": "pending",
        "created_at": time.time(), "chat_id": event.chat_id,
    }
    user_active_inr[uid] = pid
    save_data()
    clear_state(uid)

    await respond_premium(event, 
        "✅ **DEPOSIT SUBMITTED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 `{pid}`\n💰 ₹{amount:,.0f}\n📋 UPI\n\n"
        "⏳ Waiting for admin approval…\n\n"
        "📌 You'll be notified.",
        buttons=kb_main(uid), parse_mode="md")

    try:
        if photo:
            await bot.send_file(ADMIN_ID, photo,
                caption=f"📸 Payment proof • {user_mention(uid)} • <code>{pid}</code>", parse_mode="html")
        await send_premium(bot, ADMIN_ID,
            f"{e('inbox')} <b>NEW DEPOSIT</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{e('user')} {user_mention(uid)} • {e('rupee')}₹{amount:.0f} • {e('upi')} UPI\n"
            f"{e('memo')} <code>{proof_text}</code>\n{e('id')} <code>{pid}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            buttons=[
                [Button.inline(f"{e('approve')} Approve", f"adm_approve_{pid}".encode()),
                 Button.inline(f"{e('reject')} Reject", f"adm_reject_{pid}".encode())],
            ], parse_mode="html")
    except Exception as exc:
        log.warning(f"Admin notify failed: {exc}")
    log.info(f"UPI deposit: {pid} {uid} ₹{amount}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  👑  ADMIN PANEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def handle_admin_menu(event, uid):
    if uid != ADMIN_ID:
        return
    clear_state(uid)
    total = len(accounts)
    avail = sum(1 for a in accounts.values() if a["status"] == "available")
    sold = sum(1 for a in accounts.values() if a["status"] in ("reserved", "sold"))
    pend = sum(1 for p in pending_upi.values() if p["status"] == "pending")
    set_state(uid, "admin_menu")
    await respond_premium(event, 
        "👑 **ADMIN PANEL**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 Accounts: {avail}/{total}\n"
        f"🔒 Sold: {sold}\n"
        f"📥 Pending Deposits: {pend}\n"
        f"👥 Users: {len(users)}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⬇️ Select an option:",
        buttons=kb_admin_menu(), parse_mode="md")


async def handle_admin_choice(event, uid, text):
    if uid != ADMIN_ID:
        return

    if text == KB_ADM_ADD:
        set_state(uid, "adm_phone")
        await respond_premium(event, 
            "➕ **ADD ACCOUNT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📱 **Step 1:** Phone number\n📌 Example: `+919876543210`\n\n⬇️ Send phone:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if text == KB_ADM_UPI:
        set_state(uid, "adm_set_upi")
        await respond_premium(event, 
            "💳 **SET UPI ID**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Current: `{admin_settings['upi_id']}`\n\n⬇️ Enter new UPI ID:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if text == KB_ADM_BAL:
        set_state(uid, "adm_bal_uid")
        await respond_premium(event, 
            "💰 **ADD BALANCE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⬇️ Enter User ID:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if text == KB_ADM_DEPS:
        await show_next_deposit(event, uid)
        return

    if text == KB_ADM_CAST:
        set_state(uid, "adm_broadcast")
        await respond_premium(event, 
            "📡 **BROADCAST**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Will send to **{len(users)}** users\n\n⬇️ Type your message:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if text == KB_ADM_STATS:
        total_bal = sum(u2["balance"] for u2 in users.values())
        t_acc = len(accounts)
        t_avail = sum(1 for a in accounts.values() if a["status"] == "available")
        t_sold = sum(1 for a in accounts.values() if a["status"] in ("reserved", "sold"))
        t_rev = sum(a["price"] for a in accounts.values() if a["status"] in ("reserved", "sold"))
        t_deps = sum(p["amount"] for p in pending_upi.values() if p["status"] == "approved")
        countries = {}
        for a in accounts.values():
            countries[a["country"]] = countries.get(a["country"], 0) + 1

        txt = (
            "📊 **STATISTICS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Users: {len(users)}\n"
            f"💼 Total Balance: ${total_bal:,.2f}\n\n"
            f"📦 Accounts: {t_avail}/{t_acc}\n"
            f"🔒 Sold: {t_sold}\n\n"
            f"💰 Revenue: ${t_rev:,.2f}\n"
            f"📥 Deposits: ₹{t_deps:,.0f}\n"
            f"📡 OTP Listeners: {len(otp_listeners)}\n\n"
            "🌍 By Country:\n"
        )
        for c in sorted(countries):
            txt += f"  {get_flag(c)} {c.title()}: {countries[c]}\n"
        txt += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━"
        await respond_premium(event, txt, buttons=kb_admin_menu(), parse_mode="md")
        return

    if text == KB_ADM_ACCS:
        await show_next_account(event, uid)
        return

    if text == KB_ADM_BAN:
        set_state(uid, "adm_ban_uid")
        await respond_premium(event,
            "🚫 **BAN / UNBAN USER**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔴 Currently banned: **{len(banned_users)}** users\n\n"
            "📌 Send a **User ID** to ban or unban them.\n"
            "ℹ️ If already banned → unbanned. If not → banned.\n\n"
            "⬇️ Enter User ID:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    await respond_premium(event, "⚠️ Tap one of the buttons below.", buttons=kb_admin_menu(), parse_mode="md")


# ── Admin: Deposit Request Review ──

async def show_next_deposit(event, uid):
    pending = [(pid, p) for pid, p in pending_upi.items() if p["status"] == "pending"]
    if not pending:
        set_state(uid, "admin_menu")
        await respond_premium(event, 
            "📥 **DEPOSIT REQUESTS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "✅ No pending deposits.",
            buttons=kb_admin_menu(), parse_mode="md")
        return

    pid, p = pending[0]
    set_state(uid, "adm_deps_review", current_pid=pid)
    await respond_premium(event, 
        "📥 <b>DEPOSIT REQUEST</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <code>{pid}</code>\n"
        f"👤 User: {user_mention(p['user_id'])}\n"
        f"💰 Amount: ₹{p['amount']:,.0f}\n"
        f"📋 Proof: <code>{p.get('proof', 'N/A')}</code>\n"
        f"📊 Status: {p['status']}\n\n"
        f"Remaining: <b>{len(pending)}</b> pending\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Tap <b>✅ Approve</b> or <b>❌ Reject</b>",
        buttons=kb_approve_reject_back(), parse_mode="html")
    # Also send inline buttons for quick one-tap approve/reject
    await send_premium(bot, uid,
        f"⚡ <b>Quick Action</b> — <code>{pid}</code> • ₹{p['amount']:,.0f}",
        buttons=[
            [Button.inline(f"{e('approve')} Approve", f"adm_approve_{pid}".encode()),
             Button.inline(f"{e('reject')} Reject", f"adm_reject_{pid}".encode())],
        ], parse_mode="html")


async def handle_adm_deps_review(event, uid, text):
    if uid != ADMIN_ID:
        return
    st = get_st(uid)
    pid = st["data"].get("current_pid")

    if text == KB_NEXT:
        await show_next_deposit(event, uid)
        return

    if text not in (KB_APPROVE, KB_REJECT):
        await respond_premium(event, "⚠️ Tap Approve, Reject, Next, or Back.", parse_mode="md")
        return

    p = pending_upi.get(pid)
    if not p or p["status"] != "pending":
        await respond_premium(event, "⚠️ Already processed.", parse_mode="md")
        await show_next_deposit(event, uid)
        return

    if text == KB_APPROVE:
        p["status"] = "approved"
        target_uid = p["user_id"]
        amount_usd = inr_to_usd(p["amount"])
        tu = get_user(target_uid)
        tu["balance"] += amount_usd
        user_active_inr.pop(target_uid, None)
        save_data()
        try:
            await send_premium(bot, target_uid,
                "✅ **DEPOSIT APPROVED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 ₹{p['amount']:,.0f} (~${amount_usd:.2f})\n"
                f"💼 Balance: **${tu['balance']:,.2f}**\n\n🎉 Funds added!",
                buttons=kb_main(target_uid), parse_mode="md")
        except Exception:
            pass
        log.info(f"Deposit approved: {pid} ₹{p['amount']} -> {target_uid}")
        await respond_premium(event, f"✅ Approved `{pid}`!", parse_mode="md")

    elif text == KB_REJECT:
        p["status"] = "rejected"
        user_active_inr.pop(p["user_id"], None)
        save_data()
        try:
            await send_premium(bot, p["user_id"],
                "❌ **DEPOSIT REJECTED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 `{pid}` • ₹{p['amount']:,.0f}\n\nContact support if error.",
                buttons=kb_main(p["user_id"]), parse_mode="md")
        except Exception:
            pass
        log.info(f"Deposit rejected: {pid}")
        await respond_premium(event, f"❌ Rejected `{pid}`!", parse_mode="md")

    # Show next
    await show_next_deposit(event, uid)


# ── Admin: All Accounts Review ──

async def show_next_account(event, uid):
    acc_list = [(aid, a) for aid, a in accounts.items() if a["status"] == "available"]
    if not acc_list:
        set_state(uid, "admin_menu")
        await respond_premium(event, 
            "📋 **ALL ACCOUNTS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "No available accounts.",
            buttons=kb_admin_menu(), parse_mode="md")
        return

    icons = {"available": "✅", "reserved": "⏳", "sold": "🔒", "pending_otp": "📩"}
    text = "📋 **ALL ACCOUNTS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for aid, a in list(accounts.items())[:15]:
        text += f"{icons.get(a['status'], '❓')} `{aid}` {get_flag(a['country'])} `{a['phone']}` ${a['price']:.0f}\n"
    text += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Show first available for deletion
    first_aid, first_acc = acc_list[0]
    text += f"\n\n🗑 **Delete:** `{first_aid}` — `{first_acc['phone']}`\n"
    text += "Tap 🗑 Delete · ✏️ Edit Country · ▶️ Next"

    set_state(uid, "adm_accs_review", current_acc_id=first_aid, acc_index=0)
    await respond_premium(event, text, buttons=kb_delete_back(), parse_mode="md")


async def handle_adm_accs_review(event, uid, text):
    if uid != ADMIN_ID:
        return
    st = get_st(uid)

    if text == KB_NEXT:
        idx = st["data"].get("acc_index", 0) + 1
        acc_list = [(aid, a) for aid, a in accounts.items() if a["status"] == "available"]
        if idx >= len(acc_list):
            idx = 0
        if not acc_list:
            await respond_premium(event, "📋 No available accounts.", buttons=kb_admin_menu(), parse_mode="md")
            set_state(uid, "admin_menu")
            return
        aid, acc = acc_list[idx]
        set_state(uid, "adm_accs_review", current_acc_id=aid, acc_index=idx)
        await respond_premium(event, 
            f"📋 **Account {idx+1}/{len(acc_list)}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 `{aid}`\n📱 `{acc['phone']}`\n"
            f"{get_flag(acc['country'])} {acc['country'].title()}\n"
            f"💰 ${acc['price']:.2f}\n\n"
            "Tap 🗑 Delete · ✏️ Edit Country · ▶️ Next",
            buttons=kb_delete_back(), parse_mode="md")
        return

    if text == KB_EDIT_COUNTRY:
        acc_id = st["data"].get("current_acc_id")
        acc = accounts.get(acc_id)
        if not acc:
            await respond_premium(event, "❌ Account not found.", parse_mode="md")
            await show_next_account(event, uid)
            return
        set_state(uid, "adm_edit_country", edit_acc_id=acc_id)
        await respond_premium(event, 
            "✏️ **EDIT COUNTRY**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 `{acc_id}`\n📱 `{acc['phone']}`\n"
            f"🌍 Current: {get_flag(acc['country'])} **{acc['country'].title()}**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Send the correct country name or ISO code:\n\n"
            "📌 Examples:\n"
            "  `India` or `IN`\n"
            "  `Kenya` or `KE`\n"
            "  `United States` or `US`\n\n"
            "⬇️ Send country:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if text == KB_DELETE:
        acc_id = st["data"].get("current_acc_id")
        acc = accounts.pop(acc_id, None)
        if acc:
            try:
                temp = TelegramClient(StringSession(acc["session_string"]), API_ID, API_HASH)
                await temp.connect()
                if await temp.is_user_authorized():
                    await temp.log_out()
                await temp.disconnect()
            except Exception:
                pass
            save_data()
            log.info(f"Deleted: {acc_id}")
            await respond_premium(event, f"✅ Deleted `{acc_id}`!", parse_mode="md")
        else:
            await respond_premium(event, "❌ Not found.", parse_mode="md")
        await show_next_account(event, uid)
        return


async def handle_adm_edit_country(event, uid, text):
    """Handle country editing for an existing account."""
    if uid != ADMIN_ID:
        return
    st = get_st(uid)
    acc_id = st["data"].get("edit_acc_id")
    acc = accounts.get(acc_id)
    if not acc:
        clear_state(uid)
        await respond_premium(event, "❌ Account not found.", buttons=kb_admin_menu(), parse_mode="md")
        set_state(uid, "admin_menu")
        return

    result = normalize_country(text)
    if not result:
        await respond_premium(event, 
            f"⚠️ **Unrecognized:** `{text}`\n\n"
            "Send a valid country name or ISO code.\n"
            "📌 Examples: `India` · `IN` · `Kenya` · `KE`\n\n"
            "⬇️ Try again:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    country_name, flag = result
    old_country = acc["country"]
    acc["country"] = country_name
    save_data()
    clear_state(uid)
    set_state(uid, "admin_menu")

    await respond_premium(event, 
        "✅ **COUNTRY UPDATED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 `{acc_id}`\n📱 `{acc['phone']}`\n\n"
        f"🔄 Old: {get_flag(old_country)} {old_country.title()}\n"
        f"✅ New: {flag} **{country_name.title()}**",
        buttons=kb_admin_menu(), parse_mode="md")
    log.info(f"Country updated: {acc_id} {old_country} → {country_name}")


# ── Admin: Add Account Flow ──

async def handle_adm_phone(event, uid, text):
    if uid != ADMIN_ID:
        return
    phone = text if text.startswith("+") else "+" + text
    if len(phone) < 8 or not phone[1:].replace(" ", "").isdigit():
        await respond_premium(event, "⚠️ Invalid. Use: `+919876543210`\n\n⬇️ Try again:",
                            buttons=kb_cancel_only(), parse_mode="md")
        return

    await respond_premium(event, f"⏳ Sending OTP to `{phone}`…", parse_mode="md")

    try:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        result = await client.send_code_request(phone)
        set_state(uid, "adm_otp", phone=phone, client=client,
                  phone_code_hash=result.phone_code_hash)
        await respond_premium(event, 
            f"📨 **OTP SENT** → `{phone}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔑 **Step 2:** Enter OTP code\n\n⬇️ Send code:",
            buttons=kb_cancel_only(), parse_mode="md")
    except PhoneNumberInvalidError:
        await respond_premium(event, "❌ Invalid phone.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
        set_state(uid, "adm_phone")
    except PhoneNumberBannedError:
        await respond_premium(event, "🚫 Phone banned.\n\n⬇️ Try another:", buttons=kb_cancel_only(), parse_mode="md")
        set_state(uid, "adm_phone")
    except FloodWaitError as exc:
        await respond_premium(event, f"⏰ Wait {exc.seconds}s", buttons=kb_cancel_only(), parse_mode="md")
        set_state(uid, "adm_phone")
    except Exception as exc:
        log.error(f"OTP error: {exc}")
        await respond_premium(event, f"❌ `{str(exc)[:80]}`", buttons=kb_cancel_only(), parse_mode="md")
        set_state(uid, "adm_phone")


async def handle_adm_otp(event, uid, text):
    if uid != ADMIN_ID:
        return
    st = get_st(uid)
    code = text.replace(" ", "").replace("-", "")
    client = st["data"].get("client")
    if not client:
        await respond_premium(event, "❌ Expired. Start over.", buttons=kb_admin_menu(), parse_mode="md")
        set_state(uid, "admin_menu")
        return

    try:
        await client.sign_in(phone=st["data"]["phone"], code=code,
                             phone_code_hash=st["data"]["phone_code_hash"])
        set_state(uid, "adm_country", session_string=client.session.save())
        await respond_premium(event, 
            f"✅ **LOGIN OK** — `{st['data']['phone']}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌍 **Step 3:** Country name\n📌 `India` · `USA` · `UK`\n\n⬇️ Send country:",
            buttons=kb_cancel_only(), parse_mode="md")
    except SessionPasswordNeededError:
        set_state(uid, "adm_2fa")
        await respond_premium(event, 
            f"🔐 **2FA REQUIRED** — `{st['data']['phone']}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔑 Enter 2FA password:\n\n⬇️ Send password:",
            buttons=kb_cancel_only(), parse_mode="md")
    except PhoneCodeInvalidError:
        await respond_premium(event, "❌ Wrong OTP.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
    except PhoneCodeExpiredError:
        set_state(uid, "adm_phone")
        await respond_premium(event, "⏰ OTP expired. Re-enter phone:", buttons=kb_cancel_only(), parse_mode="md")
    except FloodWaitError as exc:
        await respond_premium(event, f"⏰ Wait {exc.seconds}s", buttons=kb_cancel_only(), parse_mode="md")
    except Exception as exc:
        log.error(f"Sign-in error: {exc}")
        await respond_premium(event, f"❌ `{str(exc)[:80]}`", buttons=kb_cancel_only(), parse_mode="md")


async def handle_adm_2fa(event, uid, text):
    if uid != ADMIN_ID:
        return
    st = get_st(uid)
    client = st["data"].get("client")
    if not client:
        await respond_premium(event, "❌ Expired. Start over.", buttons=kb_admin_menu(), parse_mode="md")
        set_state(uid, "admin_menu")
        return
    try:
        await client.sign_in(password=text)
        set_state(uid, "adm_country", session_string=client.session.save(), two_fa=text)
        await respond_premium(event, 
            f"✅ **2FA OK** — `{st['data']['phone']}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌍 **Step 3:** Country\n\n⬇️ Send country:",
            buttons=kb_cancel_only(), parse_mode="md")
    except PasswordHashInvalidError:
        await respond_premium(event, "❌ Wrong password.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
    except Exception as exc:
        log.error(f"2FA error: {exc}")
        await respond_premium(event, f"❌ `{str(exc)[:80]}`", buttons=kb_cancel_only(), parse_mode="md")


async def handle_adm_country(event, uid, text):
    if uid != ADMIN_ID:
        return
    raw = text.strip()
    if len(raw) < 2:
        await respond_premium(event, "⚠️ Invalid.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
        return

    result = normalize_country(raw)
    if result:
        country, flag = result
    else:
        # Accept as-is but warn admin
        country = raw.lower()
        flag = "🌍"
        await respond_premium(event, 
            f"⚠️ `{raw}` not in database — saved as `{country}`\n"
            f"Flag: {flag} (generic)\n\n"
            "💡 You can edit it later from 📋 All Accounts.\n\n"
            "Continuing…", parse_mode="md")

    set_state(uid, "adm_price", country=country)
    await respond_premium(event, 
        f"{flag} **{country.title()}**\n\n"
        "💰 **Step 4:** Price (USD)\n📌 `2` · `5.50` · `10`\n\n⬇️ Send price:",
        buttons=kb_cancel_only(), parse_mode="md")


async def handle_adm_price(event, uid, text):
    if uid != ADMIN_ID:
        return
    try:
        price = float(text.replace("$", "").replace(",", ""))
        if price <= 0:
            raise ValueError
    except (ValueError, AttributeError):
        await respond_premium(event, "⚠️ Invalid.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
        return

    st = get_st(uid)
    d = st["data"]
    acc_id = gen_id("ACC")
    accounts[acc_id] = {
        "phone": d["phone"], "country": d["country"],
        "session_string": d["session_string"], "two_fa": d.get("two_fa"),
        "price": price, "status": "available", "buyer": None, "bought_at": None,
    }

    client = d.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    save_data()
    clear_state(uid)
    set_state(uid, "admin_menu")

    await respond_premium(event, 
        "✅ **ACCOUNT ADDED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 `{acc_id}`\n📱 `{d['phone']}`\n"
        f"{get_flag(d['country'])} {d['country'].title()}\n💰 ${price:.2f}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        buttons=kb_admin_menu(), parse_mode="md")
    log.info(f"Added: {acc_id} {d['phone']} {d['country']} ${price}")


async def handle_adm_set_upi(event, uid, text):
    if uid != ADMIN_ID:
        return
    if not text:
        await respond_premium(event, "⚠️ Invalid.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
        return
    admin_settings["upi_id"] = text
    save_data()
    log.info(f"UPI: {text}")
    # Now ask for QR code upload
    set_state(uid, "adm_upi_qr")
    qr_status = "✅ QR set" if admin_settings.get("upi_qr") else "❌ No QR"
    await respond_premium(event,
        f"✅ **UPI Updated:** `{text}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📸 Current QR: {qr_status}\n\n"
        "📤 **Upload QR code** image for users\n"
        "   or **Skip** to show UPI ID only\n\n"
        "⬇️ Send QR image or tap Skip:",
        buttons=[
            [Button.text(KB_UPLOAD_QR, resize=True), Button.text(KB_SKIP, resize=True)],
            [Button.text(KB_BACK, resize=True)],
        ], parse_mode="md")


async def handle_adm_upi_qr(event, uid, text):
    """Handle QR code upload or skip after setting UPI ID."""
    if uid != ADMIN_ID:
        return

    # Skip → no QR, just UPI text
    if text == KB_SKIP:
        clear_state(uid)
        set_state(uid, "admin_menu")
        await respond_premium(event,
            "✅ **UPI saved without QR**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{e('upi')} UPI: `{admin_settings['upi_id']}`\n"
            "📸 QR: Not set — users will see UPI ID only",
            buttons=kb_admin_menu(), parse_mode="md")
        return

    # Check if user sent a photo
    photo = event.photo
    if not photo:
        await respond_premium(event,
            "⚠️ **Send a photo/image** of your UPI QR code\n\n"
            "Or tap **Skip** to continue without QR.",
            buttons=[
                [Button.text(KB_UPLOAD_QR, resize=True), Button.text(KB_SKIP, resize=True)],
                [Button.text(KB_BACK, resize=True)],
            ], parse_mode="md")
        return

    # Download photo to local file for reliable reuse
    try:
        await bot.download_media(event.message, file=str(UPI_QR_FILE))
    except Exception as ex:
        log.error(f"QR download failed: {ex}")
        await respond_premium(event,
            "❌ **Failed to save QR image.** Try again.",
            buttons=[
                [Button.text(KB_UPLOAD_QR, resize=True), Button.text(KB_SKIP, resize=True)],
                [Button.text(KB_BACK, resize=True)],
            ], parse_mode="md")
        return

    admin_settings["upi_qr"] = True
    db_set_qr(ADMIN_ID, str(UPI_QR_FILE))
    save_data()
    clear_state(uid)
    set_state(uid, "admin_menu")
    await respond_premium(event,
        "✅ **QR Code Saved!**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{e('upi')} UPI: `{admin_settings['upi_id']}`\n"
        "📸 QR: ✅ Uploaded — users will see QR\n\n"
        "👥 All users will now see this QR when depositing via UPI.",
        buttons=kb_admin_menu(), parse_mode="md")
    log.info(f"UPI QR saved to {UPI_QR_FILE}")


async def handle_adm_bal_uid(event, uid, text):
    if uid != ADMIN_ID:
        return
    try:
        target_uid = int(text)
    except (ValueError, TypeError):
        await respond_premium(event, "⚠️ Invalid User ID.\n\n⬇️ Try again:", buttons=kb_cancel_only(), parse_mode="md")
        return
    tu = get_user(target_uid)
    set_state(uid, "adm_bal_amount", target_uid=target_uid)
    await respond_premium(event, 
        f"💰 <b>MANAGE BALANCE</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {user_mention(target_uid)} • Current: <b>${tu['balance']:,.2f}</b>\n\n"
        "📌 Commands:\n"
        "  <code>50</code> → Add $50\n"
        "  <code>-20</code> → Reduce $20\n"
        "  <code>reset</code> → Reset to $0\n\n"
        "⬇️ Enter amount or <code>reset</code>:",
        buttons=kb_cancel_only(), parse_mode="html")


async def handle_adm_bal_amount(event, uid, text):
    if uid != ADMIN_ID:
        return
    st = get_st(uid)
    target_uid = st["data"]["target_uid"]
    tu = get_user(target_uid)
    old_bal = tu["balance"]

    # Reset command
    if text.lower().strip() == "reset":
        tu["balance"] = 0.0
        save_data()
        clear_state(uid)
        set_state(uid, "admin_menu")
        await respond_premium(event, 
            f"🔄 <b>BALANCE RESET</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {user_mention(target_uid)}\n💼 Old: ${old_bal:,.2f} → New: <b>$0.00</b>",
            buttons=kb_admin_menu(), parse_mode="html")
        try:
            await send_premium(bot, target_uid,
                "🔄 **Balance Reset** by admin.\n💼 Balance: **$0.00**",
                buttons=kb_main(target_uid), parse_mode="md")
        except Exception:
            pass
        log.info(f"Balance reset: {target_uid} (was ${old_bal:.2f})")
        return

    # Amount (positive = add, negative = reduce)
    try:
        amount = float(text.replace(",", "").replace("$", ""))
        if amount == 0:
            raise ValueError
    except (ValueError, AttributeError):
        await respond_premium(event, "⚠️ Invalid. Enter a number or `reset`.\n\n⬇️ Try again:",
                            buttons=kb_cancel_only(), parse_mode="md")
        return

    tu["balance"] += amount
    if tu["balance"] < 0:
        tu["balance"] = 0.0  # Prevent negative balance
    save_data()
    clear_state(uid)
    set_state(uid, "admin_menu")

    if amount > 0:
        label = f"+${amount:.2f}"
        icon = "✅"
        title = "BALANCE ADDED"
        user_msg = f"💰 **+${amount:.2f}** added by admin!\n💼 Balance: **${tu['balance']:,.2f}**"
    else:
        label = f"-${abs(amount):.2f}"
        icon = "📉"
        title = "BALANCE REDUCED"
        user_msg = f"📉 **${abs(amount):.2f}** deducted by admin.\n💼 Balance: **${tu['balance']:,.2f}**"

    await respond_premium(event, 
        f"{icon} <b>{title}</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {user_mention(target_uid)} {label}\n💼 New: ${tu['balance']:,.2f}",
        buttons=kb_admin_menu(), parse_mode="html")
    try:
        await send_premium(bot, target_uid, user_msg,
            buttons=kb_main(target_uid), parse_mode="md")
    except Exception:
        pass
    log.info(f"+${amount} -> {target_uid}")


async def handle_adm_ban_uid(event, uid, text):
    """Toggle ban/unban for a user by ID."""
    if uid != ADMIN_ID:
        return
    try:
        target_uid = int(text.strip())
    except (ValueError, TypeError):
        await respond_premium(event,
            "⚠️ Invalid User ID. Must be a number.\n\n⬇️ Try again:",
            buttons=kb_cancel_only(), parse_mode="md")
        return

    if target_uid == ADMIN_ID:
        await respond_premium(event,
            "❌ You cannot ban yourself!",
            buttons=kb_admin_menu(), parse_mode="md")
        clear_state(uid)
        set_state(uid, "admin_menu")
        return

    if target_uid in banned_users:
        banned_users.discard(target_uid)
        save_data()
        clear_state(uid)
        set_state(uid, "admin_menu")
        tu = users.get(target_uid)
        name = tu.get("name", "") if tu else ""
        await respond_premium(event,
            f"✅ **USER UNBANNED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {user_mention(target_uid)}\n"
            f"📊 Status: 🟢 **Active**\n\n"
            "User can now use the bot again.",
            buttons=kb_admin_menu(), parse_mode="html")
        try:
            await send_premium(bot, target_uid,
                "✅ **You have been unbanned.**\n\nYou can now use the bot again.",
                buttons=kb_main(target_uid), parse_mode="md")
        except Exception:
            pass
        log.info(f"Unbanned: {target_uid}")
    else:
        banned_users.add(target_uid)
        save_data()
        clear_state(uid)
        set_state(uid, "admin_menu")
        await respond_premium(event,
            f"🚫 **USER BANNED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {user_mention(target_uid)}\n"
            f"📊 Status: 🔴 **Banned**\n\n"
            "User will see a ban message on every message.",
            buttons=kb_admin_menu(), parse_mode="html")
        try:
            await send_premium(bot, target_uid,
                "🚫 **You have been banned.**\n\nContact support if you believe this is a mistake.",
                parse_mode="md")
        except Exception:
            pass
        log.info(f"Banned: {target_uid}")


async def handle_adm_broadcast(event, uid, text):
    if uid != ADMIN_ID:
        return
    if not text:
        await respond_premium(event, "⚠️ Empty.\n\n⬇️ Type message:", buttons=kb_cancel_only(), parse_mode="md")
        return
    clear_state(uid)

    all_uids = list(users.keys())
    total = len(all_uids)
    await respond_premium(event,
        f"📡 **BROADCASTING…**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Sending to **{total}** users…\n⚡ Running at full speed.",
        parse_mode="md")

    BATCH = 25          # concurrent coroutines per batch
    DELAY = 0.035       # ~28 msg/s — safely under Telegram's 30/s limit
    sent, fail = 0, 0
    msg_text = f"📡 **BROADCAST**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{text}"

    async def _send_one(tuid):
        nonlocal sent, fail
        try:
            await send_premium(bot, tuid, msg_text, parse_mode="md")
            sent += 1
        except Exception:
            fail += 1

    for i in range(0, total, BATCH):
        batch = all_uids[i:i + BATCH]
        tasks = [asyncio.create_task(_send_one(tuid)) for tuid in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(DELAY)

    set_state(uid, "admin_menu")
    await respond_premium(event,
        f"✅ **BROADCAST COMPLETE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📨 Sent: **{sent}** • ❌ Failed: **{fail}** • Total: {total}",
        buttons=kb_admin_menu(), parse_mode="md")
    log.info(f"Broadcast: {sent}/{total} ok, {fail} fail")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔘  INLINE CALLBACK HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.on(events.CallbackQuery)
async def callback_handler(event):
    uid = event.sender_id
    data = event.data.decode("utf-8") if event.data else ""
    u = get_user(uid)
    chat_id = uid  # For DMs, send to user directly (most reliable)

    try:
        # ── Home ──
        if data == "go_home":
            clear_state(uid)
            await event.answer()
            header = build_header(uid)
            try:
                await safe_edit(event, header + f"\n\n{e('check')} Returned to main menu.")
            except Exception:
                pass
            await send_premium(bot, chat_id, f"{e('home')} Main menu:", buttons=kb_main(uid))
            return

        # ── Deposit: Crypto ──
        if data == "dep_crypto":
            clear_state(uid)
            set_state(uid, "crypto_amount")
            await event.answer()
            try:
                await safe_edit(event,
                    f"{e('usdt')} **ENTER AMOUNT — USDT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Type amount of **USDT** to deposit.\n\n"
                    f"{e('pin')} Examples: `10` · `50` · `100`\n\n{e('arrow')} Send amount:",
                    buttons=[[Button.inline(f"{e('back')} Back", b"go_deposit")]])
            except Exception:
                pass
            # Send reply keyboard so user has Back/Cancel while typing
            await send_premium(bot, chat_id,
                f"{e('arrow')} Type the USDT amount below:",
                buttons=kb_back_only(), parse_mode="md")
            return

        # ── Deposit: UPI ──
        if data == "dep_upi":
            clear_state(uid)
            pid = user_active_inr.get(uid)
            if pid and pid in pending_upi and pending_upi[pid]["status"] == "pending":
                p = pending_upi[pid]
                await event.answer()
                try:
                    await safe_edit(event,
                        f"{e('warn')} **PENDING PAYMENT EXISTS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"{e('id')} `{pid}` • {e('rupee')}₹{p['amount']:,.0f}\n\nComplete or wait for approval.",
                        buttons=[[Button.inline(f"{e('back')} Back", b"go_home")]])
                except Exception:
                    pass
                return
            set_state(uid, "upi_amount")
            await event.answer()
            try:
                await safe_edit(event,
                    f"{e('upi')} **UPI DEPOSIT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{e('pin')} Min: ₹{INR_MIN_AMOUNT} · Max: ₹{INR_MAX_AMOUNT:,}\n\n"
                    f"{e('arrow')} Enter amount in ₹:",
                    buttons=[[Button.inline(f"{e('back')} Back", b"go_deposit")]])
            except Exception:
                pass
            # Send reply keyboard so user has Back/Cancel while typing
            await send_premium(bot, chat_id,
                f"{e('arrow')} Enter the ₹ amount below:",
                buttons=kb_back_only(), parse_mode="md")
            return

        # ── Deposit menu (back to) ──
        if data == "go_deposit":
            clear_state(uid)
            set_state(uid, "deposit_menu")
            await event.answer()
            try:
                await safe_edit(event,
                    f"{e('deposit')} **DEPOSIT FUNDS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Select deposit method:\n\n"
                    f"{e('usdt')}  **Crypto** — USDT auto-verify\n"
                    f"{e('upi')}  **UPI** — INR manual transfer\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    buttons=[
                        [Button.inline(f"{e('usdt')} Crypto (USDT)", b"dep_crypto"), Button.inline(f"{e('rupee')} UPI (INR)", b"dep_upi")],
                        [Button.inline(f"{e('back')} Back", b"go_home")],
                    ])
            except Exception:
                pass
            return

        # ── Buy: Back to country list ──
        if data == "go_buy":
            await event.answer()
            await handle_buy_menu(event, uid, u, edit=True)
            return

        # ── Buy: Country selected ──
        if data.startswith("buy_"):
            country = data[4:]
            avail = [(aid, a) for aid, a in accounts.items()
                     if a["status"] == "available" and a["country"] == country]
            if not avail:
                await event.answer("❌ Out of stock!", alert=True)
                return
            avail.sort(key=lambda x: x[1]["price"])
            acc_id, acc = avail[0]
            f = get_flag(country)

            text = (
                f"{e('cart')} **CONFIRM PURCHASE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{f}  **Country:** {country.title()}\n"
                f"{e('phone')}  **Phone:** `{acc['phone']}`\n"
                f"{e('dollar')}  **Price:** ${acc['price']:.2f}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{e('briefcase')}  Your Balance: ${u['balance']:,.2f}\n"
            )
            if u["balance"] < acc["price"]:
                text += f"\n{e('warn')} **Insufficient balance!**\n{e('bulb')} Deposit funds first."
                await safe_edit(event, text,
                    buttons=[[Button.inline(f"{e('deposit')} Deposit", b"go_deposit"), Button.inline(f"{e('back')} Back", b"go_buy")]])
                await event.answer()
                return

            text += f"{e('down')}  After: ${u['balance'] - acc['price']:.2f}\n\n"
            text += f"{e('bolt')} OTP auto-delivered after purchase."
            set_state(uid, "buy_confirm", acc_id=acc_id)
            await safe_edit(event, text, buttons=[
                [Button.inline(f"{e('confirm')} Confirm Purchase", f"confirm_{acc_id}".encode()),
                 Button.inline(f"{e('cancel')} Cancel", b"go_buy")],
            ])
            await event.answer()
            return


        # ── Buy: Confirm purchase ──
        if data.startswith("confirm_"):
            acc_id = data[8:]
            acc = accounts.get(acc_id)
            if not acc or acc["status"] != "available":
                await event.answer("⚠️ Account no longer available!", alert=True)
                clear_state(uid)
                return
            if u["balance"] < acc["price"]:
                await event.answer("❌ Insufficient balance!", alert=True)
                clear_state(uid)
                return

            async with _purchase_lock:
                if acc["status"] != "available" or u["balance"] < acc["price"]:
                    await event.answer("⚠️ Failed — try again.", alert=True)
                    clear_state(uid)
                    return
                acc["status"] = "pending_otp"
                acc["buyer"] = uid
                acc["bought_at"] = time.time()
                save_data()

            phone = acc["phone"]
            f = get_flag(acc["country"])
            clear_state(uid)

            await safe_edit(event,
                "⏳ **CONNECTING TO ACCOUNT…**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{f}  {acc['country'].title()}\n"
                f"📱  `{phone}`\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ Waiting for OTP delivery…\n"
                "📩 Code will appear here automatically.\n\n"
                "💰 **Balance charged after OTP confirmed.**\n\n"
                f"⏱  Valid for **{SESSION_EXPIRY_HOURS} hours** after OTP")
            await event.answer("⚡ Connecting...")

            asyncio.create_task(start_otp_listener(bot, acc_id, uid))
            log.info(f"Locked for OTP: {acc_id} -> {uid} ${acc['price']}")
            return

        # ── Admin: Approve deposit (inline on notification) ──
        if data.startswith("adm_approve_"):
            if uid != ADMIN_ID:
                await event.answer("🚫 Admin only.", alert=True)
                return
            pid = data[12:]
            p = pending_upi.get(pid)
            if not p or p["status"] != "pending":
                await event.answer("Already processed.", alert=True)
                await safe_edit(event, "✅ Already processed.")
                return
            p["status"] = "approved"
            target_uid = p["user_id"]
            amount_usd = inr_to_usd(p["amount"])
            tu = get_user(target_uid)
            tu["balance"] += amount_usd
            user_active_inr.pop(target_uid, None)
            save_data()
            await safe_edit(event,
                f"{e('approve')} <b>DEPOSIT APPROVED</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{e('id')} <code>{pid}</code>\n{e('user')} {user_mention(target_uid)}\n"
                f"{e('rupee')} ₹{p['amount']:,.0f} (~${amount_usd:.2f})\n\n"
                f"{e('check')} Credited to user.", parse_mode="html")
            try:
                await send_premium(bot, target_uid,
                    "✅ **DEPOSIT APPROVED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💰 ₹{p['amount']:,.0f} (~${amount_usd:.2f})\n"
                    f"💼 Balance: **${tu['balance']:,.2f}**\n\n🎉 Funds added!",
                    buttons=kb_main(target_uid), parse_mode="md")
            except Exception:
                pass
            log.info(f"Deposit approved (inline): {pid}")
            await event.answer("✅ Approved!")
            return

        # ── Admin: Reject deposit (inline on notification) ──
        if data.startswith("adm_reject_"):
            if uid != ADMIN_ID:
                await event.answer("🚫 Admin only.", alert=True)
                return
            pid = data[11:]
            p = pending_upi.get(pid)
            if not p or p["status"] != "pending":
                await event.answer("Already processed.", alert=True)
                await safe_edit(event, "❌ Already processed.")
                return
            p["status"] = "rejected"
            user_active_inr.pop(p["user_id"], None)
            save_data()
            await safe_edit(event,
                f"{e('reject')} <b>DEPOSIT REJECTED</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{e('id')} <code>{pid}</code>\n{e('user')} {user_mention(p['user_id'])}\n"
                f"{e('rupee')} ₹{p['amount']:,.0f}\n\n"
                f"{e('cross')} Rejected.", parse_mode="html")
            try:
                await send_premium(bot, p["user_id"],
                    "❌ **DEPOSIT REJECTED**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🆔 `{pid}` • ₹{p['amount']:,.0f}\n\nContact support if error.",
                    buttons=kb_main(p["user_id"]), parse_mode="md")
            except Exception:
                pass
            log.info(f"Deposit rejected (inline): {pid}")
            await event.answer("❌ Rejected.")
            return

    except Exception as exc:
        log.error(f"Callback error [{data}]: {exc}")
        log.error(traceback.format_exc())
        try:
            await event.answer("Error occurred.", alert=True)
        except Exception:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🚀  STARTUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    load_data()
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()

    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info(f"PREMIUM ACCOUNT MARKET")
    log.info(f"Bot: @{me.username}")
    log.info(f"Admin: {ADMIN_ID}")
    log.info(f"Accounts: {len(accounts)}")
    log.info(f"Users: {len(users)}")
    log.info(f"Credited invoices: {len(credited_invoices)}")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    asyncio.create_task(poll_invoices())
    asyncio.create_task(poll_inr_expiry())
    asyncio.create_task(session_expiry_task())
    asyncio.create_task(autosave_task())

    for acc_id, acc in list(accounts.items()):
        if acc["status"] == "pending_otp" and acc.get("buyer"):
            elapsed_min = (time.time() - (acc.get("bought_at") or 0)) / 60
            if elapsed_min < 10:
                asyncio.create_task(start_otp_listener(bot, acc_id, acc["buyer"]))
                log.info(f"Resumed OTP: {acc_id}")
            else:
                acc["status"] = "available"
                acc["buyer"] = None
                acc["bought_at"] = None
                save_data()
                log.info(f"OTP expired on restart: {acc_id}")

    log.info("Bot ready!")

    try:
        await bot.run_until_disconnected()
    finally:
        for acc_id in list(otp_listeners.keys()):
            await stop_otp_listener(acc_id)
        if _http and not _http.closed:
            await _http.close()
        save_data()
        log.info("Stopped.")


if __name__ == "__main__":
    asyncio.run(main())
