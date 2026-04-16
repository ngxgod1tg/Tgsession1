"""
╔══════════════════════════════════════════════════════════════════╗
║       💾  JSON BACKUP & RECOVERY SYSTEM                          ║
║       Backup-Only — Never used for reads in normal flow         ║
╠══════════════════════════════════════════════════════════════════╣
║  backup_all()        → Dump all Mongo collections to JSON       ║
║  recover_from_backup()→ Load JSON into empty Mongo              ║
║  check_and_recover() → Startup: test Mongo, recover if needed   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import logging
import time
from pathlib import Path

log = logging.getLogger("bot")

BACKUP_FILE = Path("backup.json")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💾  BACKUP: Mongo → JSON
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def backup_all():
    """
    Dump ALL MongoDB collections into backup.json.
    Called after every write operation (by database.py).
    """
    try:
        from database import (
            col_users, col_accounts, col_pending_upi,
            col_settings, col_credited, col_history,
            col_banned, col_qr_store,
        )

        data = {
            "users": list(col_users().find({}, {"_id": 0})),
            "accounts": list(col_accounts().find({}, {"_id": 0})),
            "pending_upi": list(col_pending_upi().find({}, {"_id": 0})),
            "settings": list(col_settings().find({}, {"_id": 0})),
            "credited_invoices": list(col_credited().find({}, {"_id": 0})),
            "purchase_history": list(col_history().find({}, {"_id": 0})),
            "banned_users": list(col_banned().find({}, {"_id": 0})),
            "qr_store": list(col_qr_store().find({}, {"_id": 0})),
            "_backup_time": time.time(),
        }

        BACKUP_FILE.write_text(json.dumps(data, indent=2, default=str))

    except Exception as exc:
        log.error(f"Backup failed: {exc}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔄  RECOVERY: JSON → Mongo
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def recover_from_backup() -> bool:
    """
    Load backup.json and insert into MongoDB.
    Returns True if recovery succeeded.
    """
    if not BACKUP_FILE.exists():
        log.warning("No backup.json found — cannot recover")
        return False

    try:
        raw = json.loads(BACKUP_FILE.read_text())
    except Exception as exc:
        log.error(f"Backup JSON parse failed: {exc}")
        return False

    try:
        from database import (
            col_users, col_accounts, col_pending_upi,
            col_settings, col_credited, col_history,
            col_banned, col_qr_store,
        )

        restored = 0

        # Users
        users_data = raw.get("users", [])
        if users_data:
            col_users().delete_many({})  # Clear before restore
            col_users().insert_many(users_data)
            restored += len(users_data)
            log.info(f"  Restored {len(users_data)} users")

        # Accounts
        accounts_data = raw.get("accounts", [])
        if accounts_data:
            col_accounts().delete_many({})
            col_accounts().insert_many(accounts_data)
            restored += len(accounts_data)
            log.info(f"  Restored {len(accounts_data)} accounts")

        # Pending UPI
        upi_data = raw.get("pending_upi", [])
        if upi_data:
            col_pending_upi().delete_many({})
            col_pending_upi().insert_many(upi_data)
            restored += len(upi_data)
            log.info(f"  Restored {len(upi_data)} UPI deposits")

        # Settings
        settings_data = raw.get("settings", [])
        if settings_data:
            col_settings().delete_many({})
            col_settings().insert_many(settings_data)
            restored += len(settings_data)
            log.info(f"  Restored {len(settings_data)} settings")

        # Credited invoices
        credited_data = raw.get("credited_invoices", [])
        if credited_data:
            col_credited().delete_many({})
            col_credited().insert_many(credited_data)
            restored += len(credited_data)
            log.info(f"  Restored {len(credited_data)} credited invoices")

        # Purchase history
        history_data = raw.get("purchase_history", [])
        if history_data:
            col_history().delete_many({})
            col_history().insert_many(history_data)
            restored += len(history_data)
            log.info(f"  Restored {len(history_data)} purchase records")

        # Banned users
        banned_data = raw.get("banned_users", [])
        if banned_data:
            col_banned().delete_many({})
            col_banned().insert_many(banned_data)
            restored += len(banned_data)
            log.info(f"  Restored {len(banned_data)} banned users")

        # QR store
        qr_data = raw.get("qr_store", [])
        if qr_data:
            col_qr_store().delete_many({})
            col_qr_store().insert_many(qr_data)
            restored += len(qr_data)
            log.info(f"  Restored {len(qr_data)} QR records")

        log.info(f"✅ Recovered from JSON backup ({restored} total records)")
        return True

    except Exception as exc:
        log.error(f"Recovery failed: {exc}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔄  LEGACY MIGRATION: bot_store_data.json → Mongo
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LEGACY_DATA_FILE = Path("bot_store_data.json")


def migrate_legacy_data() -> bool:
    """
    One-time migration from the old bot_store_data.json format into MongoDB.
    Returns True if migration was performed.
    """
    if not LEGACY_DATA_FILE.exists():
        return False

    try:
        raw = json.loads(LEGACY_DATA_FILE.read_text())
    except Exception as exc:
        log.error(f"Legacy JSON parse failed: {exc}")
        return False

    try:
        from database import (
            col_users, col_accounts, col_pending_upi,
            col_settings, col_credited, col_history,
            col_banned,
        )

        migrated = 0

        # Users: old format is {"uid_str": {data}}
        old_users = raw.get("users", {})
        if old_users and isinstance(old_users, dict):
            for uid_str, data in old_users.items():
                data["user_id"] = int(uid_str)
                col_users().update_one(
                    {"user_id": data["user_id"]},
                    {"$set": data},
                    upsert=True,
                )
                migrated += 1
            log.info(f"  Migrated {len(old_users)} users")

        # Accounts: old format is {"acc_id": {data}}
        # Also merge session strings from accounts_sessions
        old_accounts = raw.get("accounts", {})
        old_sessions = raw.get("accounts_sessions", {})
        if old_accounts and isinstance(old_accounts, dict):
            for acc_id, data in old_accounts.items():
                data["acc_id"] = acc_id
                data["session_string"] = old_sessions.get(acc_id, data.get("session_string", ""))
                col_accounts().update_one(
                    {"acc_id": acc_id},
                    {"$set": data},
                    upsert=True,
                )
                migrated += 1
            log.info(f"  Migrated {len(old_accounts)} accounts")

        # Pending UPI: old format is {"pid": {data}}
        old_upi = raw.get("pending_upi", {})
        if old_upi and isinstance(old_upi, dict):
            for pid, data in old_upi.items():
                data["pid"] = pid
                col_pending_upi().update_one(
                    {"pid": pid},
                    {"$set": data},
                    upsert=True,
                )
                migrated += 1
            log.info(f"  Migrated {len(old_upi)} UPI deposits")

        # Settings
        old_settings = raw.get("settings", {})
        if old_settings and isinstance(old_settings, dict):
            for key, value in old_settings.items():
                col_settings().update_one(
                    {"key": key},
                    {"$set": {"key": key, "value": value}},
                    upsert=True,
                )
                migrated += 1

        # Credited invoices
        old_credited = raw.get("credited_invoices", [])
        if old_credited and isinstance(old_credited, list):
            for track_id in old_credited:
                col_credited().update_one(
                    {"track_id": track_id},
                    {"$set": {"track_id": track_id}},
                    upsert=True,
                )
                migrated += 1

        # Purchase history: old format is {"uid_str": [records]}
        old_history = raw.get("purchase_history", {})
        if old_history and isinstance(old_history, dict):
            for uid_str, records in old_history.items():
                uid = int(uid_str)
                for record in records:
                    record["user_id"] = uid
                    # Avoid duplicates by checking acc_id
                    col_history().update_one(
                        {"user_id": uid, "acc_id": record.get("acc_id", "")},
                        {"$set": record},
                        upsert=True,
                    )
                    migrated += 1

        # Banned users
        old_banned = raw.get("banned_users", [])
        if old_banned and isinstance(old_banned, list):
            for uid in old_banned:
                col_banned().update_one(
                    {"user_id": int(uid)},
                    {"$set": {"user_id": int(uid)}},
                    upsert=True,
                )
                migrated += 1

        log.info(f"✅ Legacy migration complete ({migrated} records)")

        # Rename legacy file so it's not re-migrated
        legacy_done = LEGACY_DATA_FILE.with_suffix(".json.migrated")
        LEGACY_DATA_FILE.rename(legacy_done)
        log.info(f"  Renamed {LEGACY_DATA_FILE} → {legacy_done.name}")

        return True

    except Exception as exc:
        log.error(f"Legacy migration failed: {exc}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🚀  STARTUP: Check & Recover
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def check_and_recover():
    """
    Called on bot startup.
    1. Test Mongo connection
    2. If Mongo fails → attempt recovery from JSON
    3. If Mongo OK but empty → attempt recovery from JSON OR legacy migration
    4. Ensure indexes
    """
    from database import ping_mongo, ensure_indexes, col_users, col_accounts

    log.info("━━━ Database Startup ━━━")

    # Step 1: Test connection
    if not ping_mongo():
        log.error("❌ MongoDB connection FAILED")
        log.info("Attempting recovery from backup.json…")
        # Try again after a moment — maybe Mongo is starting up
        import time as _t
        _t.sleep(2)
        if not ping_mongo():
            log.critical("MongoDB is unreachable. Bot cannot start without database.")
            raise ConnectionError("MongoDB is unreachable and no fallback is possible at runtime.")

    log.info("✅ MongoDB connected")

    # Step 2: Ensure indexes
    ensure_indexes()

    # Step 3: Check if Mongo is empty
    user_count = col_users().count_documents({})
    account_count = col_accounts().count_documents({})

    if user_count == 0 and account_count == 0:
        log.info("MongoDB is empty — checking for data to restore…")

        # Try legacy migration first (from bot_store_data.json)
        if LEGACY_DATA_FILE.exists():
            log.info("Found legacy bot_store_data.json — migrating…")
            if migrate_legacy_data():
                # Create initial backup after migration
                backup_all()
                return

        # Try recovery from backup.json
        if BACKUP_FILE.exists():
            log.info("Found backup.json — recovering…")
            if recover_from_backup():
                return

        log.info("No existing data found — starting fresh")
    else:
        log.info(f"MongoDB has data: {user_count} users, {account_count} accounts")

        # Still ensure backup.json exists
        if not BACKUP_FILE.exists():
            log.info("Creating initial backup.json…")
            backup_all()
