"""
╔══════════════════════════════════════════════════════════════════╗
║       🗄️  MONGODB DATABASE LAYER                                ║
║       Primary Storage — All reads/writes go through here        ║
╠══════════════════════════════════════════════════════════════════╣
║  MongoDB = PRIMARY database (ALL runtime reads/writes)          ║
║  JSON    = BACKUP only (auto-updated after every write)         ║
║  Every write:  Mongo → then backup_all() to JSON               ║
║  Every read:   Mongo ONLY (JSON never touched in normal flow)   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import logging
import os
import time
from functools import wraps

from pymongo import MongoClient, ASCENDING
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    PyMongoError,
)

log = logging.getLogger("bot")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔌  CONNECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError(
        "Required environment variable 'MONGO_URI' is not set.\n"
        "Atlas example: mongodb+srv://user:pass@cluster.mongodb.net/bot_db?retryWrites=true&w=majority\n"
        "Local example: mongodb://localhost:27017"
    )

_client: MongoClient | None = None
_db = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
        )
    return _client


def get_db():
    global _db
    if _db is None:
        _db = get_client()["bot_db"]
    return _db


def ping_mongo() -> bool:
    """Test MongoDB connectivity. Returns True if reachable."""
    try:
        get_client().admin.command("ping")
        return True
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        log.error(f"MongoDB ping failed: {exc}")
        return False
    except Exception as exc:
        log.error(f"MongoDB ping unexpected error: {exc}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📦  COLLECTION REFERENCES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def col_users():
    return get_db()["users"]

def col_accounts():
    return get_db()["accounts"]

def col_pending_upi():
    return get_db()["pending_upi"]

def col_settings():
    return get_db()["settings"]

def col_credited():
    return get_db()["credited_invoices"]

def col_history():
    return get_db()["purchase_history"]

def col_banned():
    return get_db()["banned_users"]

def col_qr_store():
    return get_db()["qr_store"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🏗️  INDEX SETUP (called once on startup)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def ensure_indexes():
    """Create unique indexes for data integrity."""
    try:
        col_users().create_index("user_id", unique=True)
        col_accounts().create_index("acc_id", unique=True)
        col_pending_upi().create_index("pid", unique=True)
        col_settings().create_index("key", unique=True)
        col_credited().create_index("track_id", unique=True)
        col_history().create_index([("user_id", ASCENDING), ("acc_id", ASCENDING)])
        col_banned().create_index("user_id", unique=True)
        col_qr_store().create_index("user_id", unique=True)
        log.info("MongoDB indexes ensured")
    except PyMongoError as exc:
        log.error(f"Index creation failed: {exc}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔒  SAFE WRAPPER (prevents crashes on DB errors)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Lazy import to avoid circular dependency
_backup_fn = None

def _trigger_backup():
    """Call backup_all() after writes. Lazy-imported to avoid circular imports."""
    global _backup_fn
    if _backup_fn is None:
        try:
            from backup import backup_all
            _backup_fn = backup_all
        except ImportError:
            log.warning("backup.py not found — JSON backup disabled")
            _backup_fn = lambda: None
    try:
        _backup_fn()
    except Exception as exc:
        log.warning(f"Backup failed (non-fatal): {exc}")


def safe_read(default=None):
    """Decorator: wrap a read operation, return default on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except PyMongoError as exc:
                log.error(f"DB read error [{func.__name__}]: {exc}")
                return default
            except Exception as exc:
                log.error(f"Unexpected read error [{func.__name__}]: {exc}")
                return default
        return wrapper
    return decorator


def safe_write(func):
    """Decorator: wrap a write operation, trigger backup on success."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            _trigger_backup()
            return result
        except PyMongoError as exc:
            log.error(f"DB write error [{func.__name__}]: {exc}")
            return None
        except Exception as exc:
            log.error(f"Unexpected write error [{func.__name__}]: {exc}")
            return None
    return wrapper


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  👤  USERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=None)
def db_get_user(uid: int) -> dict | None:
    doc = col_users().find_one({"user_id": uid}, {"_id": 0})
    return doc


@safe_write
def db_set_user(uid: int, data: dict):
    data["user_id"] = uid
    col_users().update_one(
        {"user_id": uid},
        {"$set": data},
        upsert=True,
    )


@safe_read(default={})
def db_get_all_users() -> dict:
    """Return {uid: data} for all users."""
    result = {}
    for doc in col_users().find({}, {"_id": 0}):
        result[doc["user_id"]] = doc
    return result


@safe_read(default=0)
def db_count_users() -> int:
    return col_users().count_documents({})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📦  ACCOUNTS (Telegram accounts for sale)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=None)
def db_get_account(acc_id: str) -> dict | None:
    doc = col_accounts().find_one({"acc_id": acc_id}, {"_id": 0})
    return doc


@safe_write
def db_set_account(acc_id: str, data: dict):
    data["acc_id"] = acc_id
    col_accounts().update_one(
        {"acc_id": acc_id},
        {"$set": data},
        upsert=True,
    )


@safe_write
def db_delete_account(acc_id: str):
    col_accounts().delete_one({"acc_id": acc_id})


@safe_read(default={})
def db_get_all_accounts() -> dict:
    """Return {acc_id: data} for all accounts."""
    result = {}
    for doc in col_accounts().find({}, {"_id": 0}):
        result[doc["acc_id"]] = doc
    return result


@safe_read(default=[])
def db_get_available_accounts(country: str | None = None) -> list[tuple[str, dict]]:
    """Return [(acc_id, data)] for available accounts, optionally filtered by country."""
    query = {"status": "available"}
    if country:
        query["country"] = country
    result = []
    for doc in col_accounts().find(query, {"_id": 0}):
        result.append((doc["acc_id"], doc))
    return result


@safe_read(default=[])
def db_get_accounts_by_buyer(uid: int) -> list[tuple[str, dict]]:
    """Return [(acc_id, data)] for accounts bought by a user."""
    result = []
    for doc in col_accounts().find({"buyer": uid}, {"_id": 0}):
        result.append((doc["acc_id"], doc))
    return result


@safe_read(default=0)
def db_count_accounts(status: str | None = None) -> int:
    query = {}
    if status:
        query["status"] = status
    return col_accounts().count_documents(query)


@safe_read(default=0)
def db_count_accounts_multi_status(statuses: list[str]) -> int:
    return col_accounts().count_documents({"status": {"$in": statuses}})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💰  PENDING UPI DEPOSITS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=None)
def db_get_upi(pid: str) -> dict | None:
    return col_pending_upi().find_one({"pid": pid}, {"_id": 0})


@safe_write
def db_set_upi(pid: str, data: dict):
    data["pid"] = pid
    col_pending_upi().update_one(
        {"pid": pid},
        {"$set": data},
        upsert=True,
    )


@safe_read(default=[])
def db_get_pending_upis() -> list[tuple[str, dict]]:
    """Return [(pid, data)] for pending UPI deposits."""
    result = []
    for doc in col_pending_upi().find({"status": "pending"}, {"_id": 0}):
        result.append((doc["pid"], doc))
    return result


@safe_read(default=[])
def db_get_all_upis() -> list[tuple[str, dict]]:
    """Return [(pid, data)] for all UPI deposits."""
    result = []
    for doc in col_pending_upi().find({}, {"_id": 0}):
        result.append((doc["pid"], doc))
    return result


@safe_read(default=0)
def db_sum_approved_upi() -> float:
    pipeline = [
        {"$match": {"status": "approved"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
    ]
    result = list(col_pending_upi().aggregate(pipeline))
    return result[0]["total"] if result else 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⚙️  ADMIN SETTINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=None)
def db_get_setting(key: str, default=None):
    doc = col_settings().find_one({"key": key}, {"_id": 0})
    return doc["value"] if doc else default


@safe_write
def db_set_setting(key: str, value):
    col_settings().update_one(
        {"key": key},
        {"$set": {"key": key, "value": value}},
        upsert=True,
    )


@safe_read(default={})
def db_get_all_settings() -> dict:
    result = {}
    for doc in col_settings().find({}, {"_id": 0}):
        result[doc["key"]] = doc["value"]
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔒  CREDITED INVOICES (double-credit prevention)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=False)
def db_is_credited(track_id: str) -> bool:
    return col_credited().find_one({"track_id": track_id}) is not None


@safe_write
def db_add_credited(track_id: str):
    col_credited().update_one(
        {"track_id": track_id},
        {"$set": {"track_id": track_id, "credited_at": time.time()}},
        upsert=True,
    )


@safe_read(default=0)
def db_count_credited() -> int:
    return col_credited().count_documents({})


@safe_read(default=[])
def db_get_all_credited() -> list[str]:
    return [doc["track_id"] for doc in col_credited().find({}, {"_id": 0})]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📋  PURCHASE HISTORY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=[])
def db_get_history(uid: int) -> list[dict]:
    return list(col_history().find({"user_id": uid}, {"_id": 0}).sort("bought_at", -1))


@safe_write
def db_add_history(uid: int, record: dict):
    record["user_id"] = uid
    col_history().update_one(
        {"user_id": uid, "acc_id": record.get("acc_id", "")},
        {"$set": record},
        upsert=True,
    )


@safe_read(default={})
def db_get_all_history() -> dict:
    """Return {uid: [records]} for all users."""
    result = {}
    for doc in col_history().find({}, {"_id": 0}):
        uid = doc["user_id"]
        if uid not in result:
            result[uid] = []
        result[uid].append(doc)
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🚫  BANNED USERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=False)
def db_is_banned(uid: int) -> bool:
    return col_banned().find_one({"user_id": uid}) is not None


@safe_write
def db_ban_user(uid: int):
    col_banned().update_one(
        {"user_id": uid},
        {"$set": {"user_id": uid, "banned_at": time.time()}},
        upsert=True,
    )


@safe_write
def db_unban_user(uid: int):
    col_banned().delete_one({"user_id": uid})


@safe_read(default=0)
def db_count_banned() -> int:
    return col_banned().count_documents({})


@safe_read(default=[])
def db_get_all_banned() -> list[int]:
    return [doc["user_id"] for doc in col_banned().find({}, {"_id": 0})]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📸  QR STORE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@safe_read(default=None)
def db_get_qr(user_id: int) -> str | None:
    doc = col_qr_store().find_one({"user_id": user_id}, {"_id": 0})
    return doc["path"] if doc else None


@safe_write
def db_set_qr(user_id: int, file_path: str):
    col_qr_store().update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "path": file_path}},
        upsert=True,
    )
