import base64
import hashlib
import hmac
import io
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

try:
    from zoneinfo import ZoneInfo
    MSK_TZ = ZoneInfo("Europe/Moscow")
except Exception:
    MSK_TZ = timezone(timedelta(hours=3))

import psycopg
import telebot
from telebot import types

# ============================================================
# КОНФИГУРАЦИЯ (Переменные окружения Render)
# ============================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
RAW_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PORT = int(os.environ.get("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN")
if not RAW_DATABASE_URL:
    raise RuntimeError("Не задана переменная окружения DATABASE_URL")


def normalize_db_url(url: str) -> str:
    u = url.strip()
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://"):]
    parsed = urlparse(u)
    host = (parsed.hostname or "").lower()
    if host not in ("localhost", "127.0.0.1", "::1") and "sslmode=" not in (parsed.query or "").lower():
        sep = "&" if parsed.query else "?"
        u = f"{u}{sep}sslmode=require"
    return u


DATABASE_URL = normalize_db_url(RAW_DATABASE_URL)

telebot.apihelper.CONNECT_TIMEOUT = 10
telebot.apihelper.READ_TIMEOUT = 30
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, threaded=True)

ALL_PERMISSIONS = [
    ("view_dashboard", "Статистика и доходы"),
    ("view_users", "Список пользователей и покупки Stars"),
    ("view_chats", "Просмотр переписок (чатов) GDZgo"),
    ("send_dm", "Личные сообщения пользователям в боте"),
    ("broadcast", "Массовая рассылка (текст и фото)"),
]
PERM_KEYS = {k for k, _ in ALL_PERMISSIONS}

REFERRAL_PREMIUM_BONUS_COINS = 30
MAX_JSON_BODY_BYTES = 8_000_000
MAX_PHOTO_RAW_BYTES = 5_500_000

_db_init_lock = threading.Lock()
_db_initialized = False


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_msk(ts_val) -> str:
    if ts_val is None or ts_val == "":
        return "—"
    try:
        if isinstance(ts_val, (int, float)):
            sec = ts_val / 1000.0 if ts_val > 10_000_000_000 else float(ts_val)
            dt = datetime.fromtimestamp(sec, tz=timezone.utc).astimezone(MSK_TZ)
            return dt.strftime("%d.%m.%Y %H:%M")
        if isinstance(ts_val, datetime):
            dt = ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
            return dt.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M")
        s = str(ts_val).strip()
        if s.isdigit():
            return fmt_msk(int(s))
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(ts_val)[:19]


def get_conn():
    return psycopg.connect(
        DATABASE_URL,
        autocommit=False,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def with_db(fn, retries: int = 2):
    ensure_db_ready()
    last_err = None
    for attempt in range(retries):
        try:
            with get_conn() as conn:
                res = fn(conn)
                return res
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            last_err = exc
            if attempt + 1 < retries:
                time.sleep(0.25)
                continue
            raise
    if last_err:
        raise last_err


def _table_exists(cur, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL;", (table_name,))
    row = cur.fetchone()
    return bool(row and row[0])


def init_bot_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_bot_users (
                    tg_user_id BIGINT PRIMARY KEY,
                    tg_username VARCHAR(120) DEFAULT '',
                    tg_first_name VARCHAR(160) DEFAULT '',
                    tg_last_name VARCHAR(160) DEFAULT '',
                    gdz_user_id INT DEFAULT 0,
                    gdz_login VARCHAR(120) DEFAULT '',
                    is_blocked BOOLEAN DEFAULT FALSE,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS tg_username VARCHAR(120) DEFAULT '';")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS tg_first_name VARCHAR(160) DEFAULT '';")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS tg_last_name VARCHAR(160) DEFAULT '';")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS gdz_user_id INT DEFAULT 0;")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS gdz_login VARCHAR(120) DEFAULT '';")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
            cur.execute("ALTER TABLE banka_bot_users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_stars_orders (
                    order_uid SERIAL PRIMARY KEY,
                    pay_code VARCHAR(16) UNIQUE NOT NULL,
                    buyer_uid INT NOT NULL DEFAULT 0,
                    buyer_login VARCHAR(120) DEFAULT '',
                    item_kind VARCHAR(32) NOT NULL,
                    plan_days INT DEFAULT 0,
                    coins_amount INT DEFAULT 0,
                    target_bg_id INT DEFAULT 0,
                    stars_amount INT NOT NULL,
                    status_tag VARCHAR(24) NOT NULL DEFAULT 'PENDING',
                    telegram_user_id BIGINT DEFAULT 0,
                    telegram_charge_id VARCHAR(160) UNIQUE NULL,
                    ui_celebrated BOOLEAN DEFAULT FALSE,
                    created_ms BIGINT NOT NULL DEFAULT 0,
                    paid_ms BIGINT DEFAULT NULL,
                    created_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_stars_invoices (
                    invoice_uid SERIAL PRIMARY KEY,
                    order_uid INT NOT NULL,
                    pay_code VARCHAR(16) DEFAULT '',
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    cleaned_flag BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (order_uid, chat_id, message_id)
                );
                """
            )
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS order_uid INT NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS pay_code VARCHAR(16) DEFAULT '';")
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS chat_id BIGINT NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS message_id BIGINT NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS cleaned_flag BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE banka_stars_invoices ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_banka_stars_invoices_clean
                ON banka_stars_invoices (cleaned_flag, order_uid);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_bot_web_admins (
                    admin_uid SERIAL PRIMARY KEY,
                    username VARCHAR(80) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(24) NOT NULL DEFAULT 'USER',
                    permissions_json TEXT NOT NULL DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TIMESTAMP NULL
                );
                """
            )
            cur.execute("ALTER TABLE banka_bot_web_admins ADD COLUMN IF NOT EXISTS role VARCHAR(24) NOT NULL DEFAULT 'USER';")
            cur.execute("ALTER TABLE banka_bot_web_admins ADD COLUMN IF NOT EXISTS permissions_json TEXT NOT NULL DEFAULT '[]';")
            cur.execute("ALTER TABLE banka_bot_web_admins ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP NULL;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_bot_web_sessions (
                    token VARCHAR(96) PRIMARY KEY,
                    admin_uid INT NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute("ALTER TABLE banka_bot_web_sessions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP + INTERVAL '7 days');")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_banka_bot_web_sessions_exp ON banka_bot_web_sessions (expires_at);")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_bot_messages_log (
                    log_uid SERIAL PRIMARY KEY,
                    sender_admin VARCHAR(80) NOT NULL,
                    target_type VARCHAR(24) NOT NULL,
                    target_tg_id BIGINT DEFAULT 0,
                    message_text TEXT NOT NULL DEFAULT '',
                    has_photo BOOLEAN DEFAULT FALSE,
                    sent_count INT DEFAULT 0,
                    failed_count INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute("ALTER TABLE banka_bot_messages_log ADD COLUMN IF NOT EXISTS has_photo BOOLEAN DEFAULT FALSE;")
        conn.commit()


def ensure_db_ready():
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        init_bot_tables()
        _db_initialized = True


def init_db_background():
    while not _db_initialized:
        try:
            ensure_db_ready()
            print("Database initialized successfully", flush=True)
            return
        except Exception as e:
            print(f"[DB INIT RETRY] {e}", flush=True)
            time.sleep(3)


# ============================================================
# БЕЗОПАСНОСТЬ ВЕБ-АДМИНКИ (scrypt + pbkdf2_sha256 из Android)
# ============================================================
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        parts = stored.split("$")
        if len(parts) != 3:
            return False
        algo, salt_hex, dk_hex = parts
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(dk_hex)
        if algo == "scrypt":
            dk = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=32)
            return hmac.compare_digest(dk, expected)
        if algo == "pbkdf2_sha256":
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000, dklen=32)
            return hmac.compare_digest(dk, expected)
        return False
    except Exception:
        return False


def parse_perms(raw_json: str) -> list[str]:
    try:
        arr = json.loads(raw_json or "[]")
        if isinstance(arr, list):
            return [str(x) for x in arr if str(x) in PERM_KEYS]
    except Exception:
        pass
    return []


def user_has_perm(admin_row: dict, perm: str) -> bool:
    if not admin_row:
        return False
    role = (admin_row.get("role") or "USER").upper()
    if role == "SUPER_ADMIN":
        return True
    if role == "MODERATOR":
        return perm in (admin_row.get("permissions") or [])
    return False


# ============================================================
# ДЕКОДИРОВАНИЕ И ПРОВЕРКА ФОТО ДЛЯ РАССЫЛКИ / ЛС
# ============================================================
def parse_image_data_url(raw_data: str):
    if not raw_data:
        return None, ""
    s = str(raw_data).strip()
    ext = "jpg"
    if s.startswith("data:"):
        if ";base64," not in s:
            raise ValueError("Некорректный формат изображения")
        header, b64_part = s.split(";base64,", 1)
        mime = header[5:].lower()
        if mime == "image/png":
            ext = "png"
        elif mime == "image/webp":
            ext = "webp"
        elif mime == "image/gif":
            ext = "gif"
        elif mime in ("image/jpeg", "image/jpg"):
            ext = "jpg"
        else:
            raise ValueError("Поддерживаются только PNG, JPEG, WEBP и GIF")
        s = b64_part
    try:
        raw_bytes = base64.b64decode(s, validate=True)
    except Exception:
        raise ValueError("Некорректные данные изображения (base64)")
    if not raw_bytes:
        raise ValueError("Пустой файл изображения")
    if len(raw_bytes) > MAX_PHOTO_RAW_BYTES:
        raise ValueError("Изображение больше 5.5 МБ")
    if raw_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        ext = "png"
    elif raw_bytes.startswith(b"\xff\xd8\xff"):
        ext = "jpg"
    elif raw_bytes.startswith((b"GIF87a", b"GIF89a")):
        ext = "gif"
    elif raw_bytes[:4] == b"RIFF" and raw_bytes[8:12] == b"WEBP":
        ext = "webp"
    else:
        raise ValueError("Файл не является корректным изображением (PNG/JPG/WEBP/GIF)")
    return raw_bytes, f"broadcast.{ext}"


def send_telegram_content(chat_id: int, text: str, photo_bytes: bytes | None, photo_name: str, cached_file_id: str | None = None):
    clean_text = (text or "").strip()
    if photo_bytes is None and not cached_file_id:
        bot.send_message(chat_id, clean_text[:4000])
        return None

    media = cached_file_id
    if media is None:
        bio = io.BytesIO(photo_bytes)
        bio.name = photo_name or "photo.jpg"
        media = bio

    if len(clean_text) <= 1024:
        sent = bot.send_photo(chat_id, media, caption=clean_text if clean_text else None)
    else:
        sent = bot.send_photo(chat_id, media, caption=None)
        bot.send_message(chat_id, clean_text[:4000])

    if cached_file_id is None and getattr(sent, "photo", None):
        return sent.photo[-1].file_id
    return cached_file_id


# ============================================================
# ЛОГИКА БОТА: СИНХРОНИЗАЦИЯ С БД GDZgo И TELEGRAM STARS
# ============================================================
def upsert_tg_user(tg_user, gdz_uid: int | None = None, gdz_login: str | None = None):
    if not tg_user:
        return

    def _op(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO banka_bot_users (
                    tg_user_id, tg_username, tg_first_name, tg_last_name,
                    gdz_user_id, gdz_login, is_blocked, started_at, last_seen_at
                )
                VALUES (%s, %s, %s, %s, COALESCE(%s, 0), COALESCE(%s, ''), FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (tg_user_id) DO UPDATE SET
                    tg_username = EXCLUDED.tg_username,
                    tg_first_name = EXCLUDED.tg_first_name,
                    tg_last_name = EXCLUDED.tg_last_name,
                    is_blocked = FALSE,
                    last_seen_at = CURRENT_TIMESTAMP,
                    gdz_user_id = CASE WHEN EXCLUDED.gdz_user_id > 0 THEN EXCLUDED.gdz_user_id ELSE banka_bot_users.gdz_user_id END,
                    gdz_login = CASE WHEN EXCLUDED.gdz_login <> '' THEN EXCLUDED.gdz_login ELSE banka_bot_users.gdz_login END;
                """,
                (
                    int(tg_user.id),
                    (tg_user.username or "")[:120],
                    (tg_user.first_name or "")[:160],
                    (tg_user.last_name or "")[:160],
                    gdz_uid if (gdz_uid and gdz_uid > 0) else None,
                    (gdz_login or "")[:120] if gdz_login else None,
                ),
            )
        conn.commit()

    with_db(_op)


def get_linked_gdz_account(tg_user_id: int):
    def _op(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT gdz_user_id, gdz_login FROM banka_bot_users WHERE tg_user_id = %s LIMIT 1;",
                (tg_user_id,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            gdz_uid = int(row[0])
            if not _table_exists(cur, "banka_profiles_vault"):
                return {"id": gdz_uid, "login": row[1] or "", "name": row[1] or "", "coins": 0, "is_vip": False}
            cur.execute(
                """
                SELECT rec_uid, entry_tag,
                       COALESCE(NULLIF(TRIM(title_first || ' ' || title_last), ''), entry_tag) AS full_name,
                       COALESCE(vault_tokens, 0) AS coins,
                       (COALESCE(vip_flag, FALSE) AND (premium_until_ms IS NULL OR premium_until_ms > %s)) AS is_vip
                FROM banka_profiles_vault
                WHERE rec_uid = %s LIMIT 1;
                """,
                (now_ms(), gdz_uid),
            )
            u = cur.fetchone()
            if not u:
                return None
            return {"id": int(u[0]), "login": u[1], "name": u[2], "coins": int(u[3]), "is_vip": bool(u[4])}

    return with_db(_op)


def fetch_shop_config() -> dict:
    cfg = {
        "stars_10d_price": 75,
        "stars_30d_price": 170,
        "coins_per_star": 2.0,
        "prem_discount_pct": 0,
    }
    try:
        def _op(conn):
            with conn.cursor() as cur:
                if not _table_exists(cur, "banka_system_config"):
                    return None
                cur.execute(
                    """
                    SELECT COALESCE(stars_10d_price, 75),
                           COALESCE(stars_30d_price, 170),
                           COALESCE(coins_per_star, 2.0),
                           COALESCE(prem_discount_pct, 0)
                    FROM banka_system_config WHERE cfg_uid = 1 LIMIT 1;
                    """
                )
                return cur.fetchone()

        row = with_db(_op)
        if row:
            cfg["stars_10d_price"] = max(1, int(row[0]))
            cfg["stars_30d_price"] = max(1, int(row[1]))
            cfg["coins_per_star"] = max(0.1, float(row[2]))
            cfg["prem_discount_pct"] = max(0, min(90, int(row[3])))
    except Exception:
        pass
    return cfg


def discounted_stars(base_stars: int, discount_pct: int) -> int:
    if discount_pct <= 0:
        return max(1, int(base_stars))
    return max(1, round(base_stars * (100 - discount_pct) / 100.0))


def gen_pay_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(10))


def create_bot_stars_order(tg_user_id: int, buyer_uid: int, buyer_login: str, item_kind: str, plan_days: int, coins_amount: int, target_bg_id: int, stars_amount: int):
    cur_ms = now_ms()

    def _op(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE banka_stars_orders
                SET status_tag = 'CANCELLED'
                WHERE buyer_uid = %s AND status_tag = 'PENDING';
                """,
                (buyer_uid,),
            )
            for _ in range(10):
                code = gen_pay_code()
                cur.execute(
                    """
                    INSERT INTO banka_stars_orders (
                        pay_code, buyer_uid, buyer_login, item_kind,
                        plan_days, coins_amount, target_bg_id, stars_amount,
                        status_tag, telegram_user_id, ui_celebrated, created_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, FALSE, %s)
                    ON CONFLICT (pay_code) DO NOTHING
                    RETURNING order_uid, pay_code;
                    """,
                    (
                        code,
                        buyer_uid,
                        buyer_login[:100],
                        item_kind.upper(),
                        max(0, plan_days),
                        max(0, coins_amount),
                        max(0, target_bg_id),
                        max(1, stars_amount),
                        tg_user_id,
                        cur_ms,
                    ),
                )
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return int(row[0]), str(row[1])
        raise RuntimeError("Не удалось создать код оплаты")

    return with_db(_op)


def send_stars_invoice_for_order(chat_id: int, order_row: dict):
    order_uid = int(order_row["order_uid"])
    pay_code = str(order_row["pay_code"])
    item_kind = str(order_row["item_kind"]).upper()
    stars_amount = max(1, int(order_row["stars_amount"]))
    plan_days = int(order_row["plan_days"] or 0)
    coins_amount = int(order_row["coins_amount"] or 0)
    bg_title = order_row.get("bg_title") or ""

    if item_kind == "PREMIUM":
        title = f"GDZgo Premium ({plan_days} дн.)"
        desc = f"Подписка Premium на {plan_days} дн. для аккаунта {order_row.get('buyer_login') or ''}".strip()
        label = f"Premium {plan_days} дн."
    elif item_kind == "COINS":
        title = f"{coins_amount} монет GDZgo"
        desc = f"Зачисление {coins_amount} монет на баланс {order_row.get('buyer_login') or ''}".strip()
        label = f"{coins_amount} монет"
    elif item_kind == "BACKGROUND":
        title = f"Фон «{bg_title or 'GDZgo'}»"
        desc = f"Покупка фона профиля в GDZgo для {order_row.get('buyer_login') or ''}".strip()
        label = "Фон профиля"
    else:
        title = "Покупка GDZgo"
        desc = "Оплата через Telegram Stars"
        label = "Заказ GDZgo"

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(text=f"⭐ Оплатить {stars_amount} Stars", pay=True))
    kb.add(types.InlineKeyboardButton(text="❌ Отменить счёт", callback_data=f"cancel_order:{order_uid}"))

    sent = bot.send_invoice(
        chat_id=chat_id,
        title=title[:32],
        description=desc[:255],
        invoice_payload=f"gdz_order:{order_uid}:{pay_code}",
        provider_token="",
        currency="XTR",
        prices=[types.LabeledPrice(label=label[:32], amount=stars_amount)],
        reply_markup=kb,
    )

    def _op(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO banka_stars_invoices (order_uid, pay_code, chat_id, message_id, cleaned_flag)
                VALUES (%s, %s, %s, %s, FALSE)
                ON CONFLICT (order_uid, chat_id, message_id) DO UPDATE SET cleaned_flag = FALSE;
                """,
                (order_uid, pay_code, int(chat_id), int(sent.message_id)),
            )
        conn.commit()

    with_db(_op)


def mark_invoices_cleaned(order_uid: int, except_delete: bool = False):
    def _op(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chat_id, message_id
                FROM banka_stars_invoices
                WHERE order_uid = %s AND COALESCE(cleaned_flag, FALSE) = FALSE;
                """,
                (order_uid,),
            )
            rows = cur.fetchall()
            cur.execute(
                "UPDATE banka_stars_invoices SET cleaned_flag = TRUE WHERE order_uid = %s;",
                (order_uid,),
            )
        conn.commit()
        return rows

    rows = with_db(_op)
    if not except_delete:
        for cid, mid in rows:
            try:
                bot.delete_message(int(cid), int(mid))
            except Exception:
                pass


def main_menu_kb(linked_acc: dict | None) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("👑 Купить Premium", callback_data="menu:premium"),
        types.InlineKeyboardButton("🪙 Купить монеты", callback_data="menu:coins"),
    )
    kb.add(
        types.InlineKeyboardButton("🖼 Купить фоны", callback_data="menu:bgs"),
        types.InlineKeyboardButton("👤 Мой профиль", callback_data="menu:profile"),
    )
    link_lbl = "🔄 Сменить аккаунт GDZgo" if linked_acc else "🔗 Привязать аккаунт GDZgo"
    kb.add(types.InlineKeyboardButton(link_lbl, callback_data="menu:link"))
    return kb


@bot.message_handler(commands=["start"])
def handle_start(message):
    upsert_tg_user(message.from_user)
    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) == 2 and parts[1].startswith("pay_"):
        pay_code = parts[1][4:].strip().upper()

        def _fetch_order(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.order_uid, o.pay_code, o.buyer_uid, o.buyer_login, o.item_kind,
                           o.plan_days, o.coins_amount, o.target_bg_id, o.stars_amount, o.status_tag
                    FROM banka_stars_orders o
                    WHERE o.pay_code = %s LIMIT 1;
                    """,
                    (pay_code,),
                )
                row = cur.fetchone()
                if not row or row[9] != "PENDING":
                    return row, ""
                bg_title = ""
                if int(row[7] or 0) > 0 and _table_exists(cur, "banka_shop_backgrounds"):
                    cur.execute("SELECT title_txt FROM banka_shop_backgrounds WHERE bg_uid = %s LIMIT 1;", (int(row[7]),))
                    b = cur.fetchone()
                    if b:
                        bg_title = b[0]
                cur.execute(
                    "UPDATE banka_stars_orders SET telegram_user_id = %s WHERE order_uid = %s;",
                    (int(message.from_user.id), int(row[0])),
                )
            conn.commit()
            return row, bg_title

        row, bg_title = with_db(_fetch_order)
        if not row:
            bot.send_message(message.chat.id, "❌ Счёт с таким кодом не найден.")
            return
        if row[9] == "PAID":
            bot.send_message(message.chat.id, "✅ Этот заказ уже оплачен!")
            return
        if row[9] != "PENDING":
            bot.send_message(message.chat.id, "⚠️ Этот счёт был отменён в приложении.")
            return

        upsert_tg_user(message.from_user, gdz_uid=int(row[2]), gdz_login=row[3])
        send_stars_invoice_for_order(
            message.chat.id,
            {
                "order_uid": row[0],
                "pay_code": row[1],
                "buyer_uid": row[2],
                "buyer_login": row[3],
                "item_kind": row[4],
                "plan_days": row[5],
                "coins_amount": row[6],
                "target_bg_id": row[7],
                "stars_amount": row[8],
                "bg_title": bg_title,
            },
        )
        return

    acc = get_linked_gdz_account(message.from_user.id)
    if acc:
        text = (
            f"👋 Привет, {message.from_user.first_name or 'друг'}!\n\n"
            f"Привязанный аккаунт GDZgo: {acc['name']} (@{acc['login']})\n"
            f"🪙 Монеты: {acc['coins']} | 👑 VIP: {'Да' if acc['is_vip'] else 'Нет'}\n\n"
            f"Выбирай что хочешь купить прямо здесь:"
        )
    else:
        text = (
            f"👋 Привет, {message.from_user.first_name or 'друг'}!\n\n"
            f"Здесь можно оплатить GDZgo Premium, монеты и фоны за Telegram Stars ⭐.\n"
            f"Для покупки прямо из бота сначала привяжи свой аккаунт GDZgo командой:\n"
            f"/link <логин> <5-значный код защиты>"
        )
    bot.send_message(message.chat.id, text, reply_markup=main_menu_kb(acc))


@bot.message_handler(commands=["link"])
def handle_link_cmd(message):
    upsert_tg_user(message.from_user)
    parts = (message.text or "").strip().split()
    if len(parts) != 3:
        bot.send_message(
            message.chat.id,
            "Использование: /link <логин_GDZgo> <5-значный_код_защиты>\n"
            "Код защиты можно посмотреть в профиле в приложении GDZgo.",
        )
        return
    login_arg = parts[1].strip()
    code_arg = parts[2].strip().upper()

    def _op(conn):
        with conn.cursor() as cur:
            if not _table_exists(cur, "banka_profiles_vault"):
                return "no_table", None
            cur.execute(
                """
                SELECT rec_uid, entry_tag,
                       COALESCE(NULLIF(TRIM(title_first || ' ' || title_last), ''), entry_tag)
                FROM banka_profiles_vault
                WHERE LOWER(entry_tag) = LOWER(%s) AND UPPER(COALESCE(guard_code, '')) = %s
                LIMIT 1;
                """,
                (login_arg, code_arg),
            )
            return "ok", cur.fetchone()

    st, u = with_db(_op)
    if st == "no_table":
        bot.send_message(message.chat.id, "❌ База пользователей ещё не инициализирована.")
        return
    if not u:
        bot.send_message(message.chat.id, "❌ Неверный логин или код защиты.")
        return
    upsert_tg_user(message.from_user, gdz_uid=int(u[0]), gdz_login=u[1])
    acc = get_linked_gdz_account(message.from_user.id)
    bot.send_message(
        message.chat.id,
        f"✅ Аккаунт привязан: {u[2]} (@{u[1]})",
        reply_markup=main_menu_kb(acc),
    )


@bot.callback_query_handler(func=lambda c: True)
def handle_callbacks(call):
    upsert_tg_user(call.from_user)
    data = call.data or ""

    if data.startswith("cancel_order:"):
        try:
            oid = int(data.split(":", 1)[1])
        except Exception:
            bot.answer_callback_query(call.id, "Некорректный заказ")
            return

        def _cancel(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE banka_stars_orders SET status_tag = 'CANCELLED' WHERE order_uid = %s AND status_tag = 'PENDING';",
                    (oid,),
                )
            conn.commit()

        with_db(_cancel)
        mark_invoices_cleaned(oid, except_delete=False)
        bot.answer_callback_query(call.id, "Счёт отменён")
        return

    acc = get_linked_gdz_account(call.from_user.id)

    if data == "menu:home":
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                "🏠 Главное меню GDZgo Stars",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=main_menu_kb(acc),
            )
        except Exception:
            bot.send_message(call.message.chat.id, "🏠 Главное меню", reply_markup=main_menu_kb(acc))
        return

    if data == "menu:link":
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "🔗 Отправь команду:\n/link <логин_GDZgo> <5-значный_код_защиты>",
        )
        return

    if data == "menu:profile":
        bot.answer_callback_query(call.id)
        if not acc:
            bot.send_message(call.message.chat.id, "⚠️ Аккаунт не привязан. Используй /link <логин> <код>")
            return
        bot.send_message(
            call.message.chat.id,
            f"👤 Профиль: {acc['name']} (@{acc['login']})\n"
            f"🪙 Монет: {acc['coins']}\n"
            f"👑 Premium: {'Активен' if acc['is_vip'] else 'Нет'}",
            reply_markup=main_menu_kb(acc),
        )
        return

    if not acc:
        bot.answer_callback_query(call.id, "Сначала привяжи аккаунт через /link", show_alert=True)
        return

    cfg = fetch_shop_config()

    if data == "menu:premium":
        bot.answer_callback_query(call.id)
        s10 = discounted_stars(cfg["stars_10d_price"], cfg["prem_discount_pct"])
        s30 = discounted_stars(cfg["stars_30d_price"], cfg["prem_discount_pct"])
        disc_txt = f" (скидка {cfg['prem_discount_pct']}%!)" if cfg["prem_discount_pct"] > 0 else ""
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(
            types.InlineKeyboardButton(f"👑 10 дней — ⭐ {s10} Stars", callback_data=f"buy_prem:10:{s10}"),
            types.InlineKeyboardButton(f"👑 30 дней — ⭐ {s30} Stars", callback_data=f"buy_prem:30:{s30}"),
            types.InlineKeyboardButton("⬅️ Назад", callback_data="menu:home"),
        )
        bot.edit_message_text(
            f"👑 Покупка GDZgo Premium{disc_txt}\nАккаунт: @{acc['login']}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb,
        )
        return

    if data == "menu:coins":
        bot.answer_callback_query(call.id)
        rate = cfg["coins_per_star"]
        kb = types.InlineKeyboardMarkup(row_width=1)
        for st in (25, 50, 100, 250, 500):
            coins = max(1, int(round(st * rate)))
            kb.add(types.InlineKeyboardButton(f"🪙 {coins} монет — ⭐ {st} Stars", callback_data=f"buy_coins:{st}:{coins}"))
        kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="menu:home"))
        bot.edit_message_text(
            f"🪙 Покупка монет GDZgo (курс: {rate:g} монет за 1 ⭐)\nАккаунт: @{acc['login']}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb,
        )
        return

    if data == "menu:bgs":
        bot.answer_callback_query(call.id)
        cur_ms = now_ms()

        def _fetch_bgs(conn):
            with conn.cursor() as cur:
                if not _table_exists(cur, "banka_shop_backgrounds"):
                    return []
                cur.execute(
                    """
                    SELECT bg_uid, title_txt, COALESCE(price_stars, 0), COALESCE(discount_pct, 0)
                    FROM banka_shop_backgrounds
                    WHERE COALESCE(is_active, TRUE) = TRUE
                      AND COALESCE(price_stars, 0) > 0
                      AND (publish_at_ms IS NULL OR publish_at_ms <= %s)
                      AND (expires_ms IS NULL OR expires_ms > %s)
                      AND (max_qty IS NULL OR COALESCE(sold_count, 0) < max_qty)
                    ORDER BY bg_uid DESC
                    LIMIT 20;
                    """,
                    (cur_ms, cur_ms),
                )
                return cur.fetchall()

        bgs = with_db(_fetch_bgs)
        kb = types.InlineKeyboardMarkup(row_width=1)
        if not bgs:
            kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="menu:home"))
            bot.edit_message_text("🖼 Сейчас нет фонов за Stars в продаже.", call.message.chat.id, call.message.message_id, reply_markup=kb)
            return
        for bg_uid, title_txt, price_s, disc in bgs:
            final_s = discounted_stars(int(price_s), int(disc))
            kb.add(types.InlineKeyboardButton(f"🖼 {title_txt} — ⭐ {final_s} Stars", callback_data=f"buy_bg:{bg_uid}"))
        kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="menu:home"))
        bot.edit_message_text(
            f"🖼 Доступные фоны профиля за Stars\nАккаунт: @{acc['login']}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb,
        )
        return

    if data.startswith("buy_prem:"):
        bot.answer_callback_query(call.id)
        _, d_s, _ = data.split(":")
        days = int(d_s)
        base = cfg["stars_10d_price"] if days == 10 else cfg["stars_30d_price"]
        stars = discounted_stars(base, cfg["prem_discount_pct"])
        oid, pcode = create_bot_stars_order(call.from_user.id, acc["id"], acc["login"], "PREMIUM", days, 0, 0, stars)
        send_stars_invoice_for_order(
            call.message.chat.id,
            {"order_uid": oid, "pay_code": pcode, "buyer_uid": acc["id"], "buyer_login": acc["login"], "item_kind": "PREMIUM", "plan_days": days, "coins_amount": 0, "target_bg_id": 0, "stars_amount": stars},
        )
        return

    if data.startswith("buy_coins:"):
        bot.answer_callback_query(call.id)
        _, st_s, _ = data.split(":")
        stars = max(1, int(st_s))
        coins = max(1, int(round(stars * cfg["coins_per_star"])))
        oid, pcode = create_bot_stars_order(call.from_user.id, acc["id"], acc["login"], "COINS", 0, coins, 0, stars)
        send_stars_invoice_for_order(
            call.message.chat.id,
            {"order_uid": oid, "pay_code": pcode, "buyer_uid": acc["id"], "buyer_login": acc["login"], "item_kind": "COINS", "plan_days": 0, "coins_amount": coins, "target_bg_id": 0, "stars_amount": stars},
        )
        return

    if data.startswith("buy_bg:"):
        bg_id = int(data.split(":", 1)[1])
        cur_ms = now_ms()

        def _fetch_bg(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT title_txt, COALESCE(price_stars, 0), COALESCE(discount_pct, 0)
                    FROM banka_shop_backgrounds
                    WHERE bg_uid = %s
                      AND COALESCE(is_active, TRUE) = TRUE
                      AND COALESCE(price_stars, 0) > 0
                      AND (publish_at_ms IS NULL OR publish_at_ms <= %s)
                      AND (expires_ms IS NULL OR expires_ms > %s)
                      AND (max_qty IS NULL OR COALESCE(sold_count, 0) < max_qty)
                    LIMIT 1;
                    """,
                    (bg_id, cur_ms, cur_ms),
                )
                return cur.fetchone()

        row = with_db(_fetch_bg)
        if not row:
            bot.answer_callback_query(call.id, "Фон больше недоступен для покупки", show_alert=True)
            return
        bot.answer_callback_query(call.id)
        stars = discounted_stars(int(row[1]), int(row[2]))
        oid, pcode = create_bot_stars_order(call.from_user.id, acc["id"], acc["login"], "BACKGROUND", 0, 0, bg_id, stars)
        send_stars_invoice_for_order(
            call.message.chat.id,
            {"order_uid": oid, "pay_code": pcode, "buyer_uid": acc["id"], "buyer_login": acc["login"], "item_kind": "BACKGROUND", "plan_days": 0, "coins_amount": 0, "target_bg_id": bg_id, "stars_amount": stars, "bg_title": row[0]},
        )


@bot.pre_checkout_query_handler(func=lambda q: True)
def handle_pre_checkout(pre_checkout_query):
    payload = pre_checkout_query.invoice_payload or ""
    if not payload.startswith("gdz_order:"):
        bot.answer_pre_checkout_query(pre_checkout_query.id, ok=False, error_message="Некорректный счёт.")
        return
    try:
        _, oid_str, pcode = payload.split(":", 2)
        oid = int(oid_str)
    except Exception:
        bot.answer_pre_checkout_query(pre_checkout_query.id, ok=False, error_message="Ошибка данных счёта.")
        return

    def _check(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status_tag, stars_amount, item_kind, target_bg_id, buyer_uid
                FROM banka_stars_orders
                WHERE order_uid = %s AND pay_code = %s
                LIMIT 1;
                """,
                (oid, pcode),
            )
            row = cur.fetchone()
            if not row:
                return False, "Заказ не найден."
            status_tag, stars_amount, item_kind, target_bg_id, buyer_uid = row
            if status_tag != "PENDING":
                return False, "Этот счёт уже отменён или оплачен."
            if int(stars_amount) != int(pre_checkout_query.total_amount):
                return False, "Сумма заказа изменилась."
            if str(item_kind).upper() == "BACKGROUND" and int(target_bg_id or 0) > 0:
                if _table_exists(cur, "banka_user_backgrounds"):
                    cur.execute(
                        "SELECT 1 FROM banka_user_backgrounds WHERE owner_uid = %s AND bg_uid = %s LIMIT 1;",
                        (int(buyer_uid), int(target_bg_id)),
                    )
                    if cur.fetchone():
                        return False, "Этот фон уже есть в вашем профиле."
                if _table_exists(cur, "banka_shop_backgrounds"):
                    cur.execute(
                        """
                        SELECT 1 FROM banka_shop_backgrounds
                        WHERE bg_uid = %s
                          AND COALESCE(is_active, TRUE) = TRUE
                          AND (publish_at_ms IS NULL OR publish_at_ms <= %s)
                          AND (expires_ms IS NULL OR expires_ms > %s)
                          AND (max_qty IS NULL OR COALESCE(sold_count, 0) < max_qty)
                        LIMIT 1;
                        """,
                        (int(target_bg_id), now_ms(), now_ms()),
                    )
                    if not cur.fetchone():
                        return False, "Товар распродан или скрыт."
            return True, ""

    ok, err_msg = with_db(_check)
    bot.answer_pre_checkout_query(pre_checkout_query.id, ok=ok, error_message=err_msg if not ok else None)


@bot.message_handler(content_types=["successful_payment"])
def handle_successful_payment(message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""
    charge_id = sp.telegram_payment_charge_id
    tg_id = int(message.from_user.id)

    try:
        _, oid_str, pcode = payload.split(":", 2)
        oid = int(oid_str)
    except Exception:
        return

    cur_ms = now_ms()

    def _apply_payment(conn):
        should_refund = False
        already_done = False
        summary_text = "✅ Оплата получена!"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_uid, buyer_uid, buyer_login, item_kind, plan_days,
                       coins_amount, target_bg_id, stars_amount, status_tag, telegram_charge_id
                FROM banka_stars_orders
                WHERE order_uid = %s AND pay_code = %s
                FOR UPDATE;
                """,
                (oid, pcode),
            )
            row = cur.fetchone()
            if not row:
                should_refund = True
            else:
                _, buyer_uid, buyer_login, item_kind, plan_days, coins_amount, target_bg_id, stars_amount, status_tag, existing_charge = row
                if existing_charge == charge_id and status_tag == "PAID":
                    already_done = True
                elif status_tag != "PENDING":
                    should_refund = True
                else:
                    cur.execute(
                        """
                        UPDATE banka_stars_orders
                        SET status_tag = 'PAID',
                            telegram_user_id = %s,
                            telegram_charge_id = %s,
                            paid_ms = %s
                        WHERE order_uid = %s;
                        """,
                        (tg_id, charge_id, cur_ms, oid),
                    )
                    kind = str(item_kind).upper()
                    if kind == "PREMIUM":
                        days = max(1, int(plan_days or 30))
                        add_ms = days * 86_400_000
                        cur.execute(
                            "SELECT vip_flag, premium_until_ms, referred_by_uid FROM banka_profiles_vault WHERE rec_uid = %s FOR UPDATE;",
                            (buyer_uid,),
                        )
                        u = cur.fetchone()
                        if u:
                            vip_flag, until_ms, ref_by = u
                            if not (bool(vip_flag) and until_ms is None):
                                base_ms = max(cur_ms, int(until_ms or 0))
                                new_until = base_ms + add_ms
                                cur.execute(
                                    "UPDATE banka_profiles_vault SET vip_flag = TRUE, premium_until_ms = %s WHERE rec_uid = %s;",
                                    (new_until, buyer_uid),
                                )
                            if ref_by and int(ref_by) > 0 and int(ref_by) != int(buyer_uid):
                                cur.execute(
                                    "UPDATE banka_profiles_vault SET vault_tokens = vault_tokens + %s WHERE rec_uid = %s;",
                                    (REFERRAL_PREMIUM_BONUS_COINS, int(ref_by)),
                                )
                                cur.execute(
                                    "UPDATE banka_profiles_vault SET referral_earned_coins = COALESCE(referral_earned_coins, 0) + %s WHERE rec_uid = %s;",
                                    (REFERRAL_PREMIUM_BONUS_COINS, buyer_uid),
                                )
                                if _table_exists(cur, "banka_referral_ledger"):
                                    cur.execute(
                                        "INSERT INTO banka_referral_ledger (inviter_uid, referral_uid, reward_type, coins_delta, note_txt) VALUES (%s, %s, 'PREMIUM_BUY', %s, %s);",
                                        (int(ref_by), buyer_uid, REFERRAL_PREMIUM_BONUS_COINS, f"Premium {days} дн. (Stars)"),
                                    )
                            if _table_exists(cur, "banka_user_notices"):
                                cur.execute(
                                    "INSERT INTO banka_user_notices (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label) VALUES (%s, 'PREMIUM_GRANTED', %s, %s, '', 'Telegram Stars');",
                                    (buyer_uid, f"Активирован Premium ({days} дн.)", f"Оплачено ⭐ {stars_amount} Stars через Telegram-бота."),
                                )
                        summary_text = f"👑 Premium на {days} дн. успешно активирован для @{buyer_login}!"
                    elif kind == "COINS":
                        c_amt = max(1, int(coins_amount or 0))
                        cur.execute(
                            "UPDATE banka_profiles_vault SET vault_tokens = vault_tokens + %s WHERE rec_uid = %s;",
                            (c_amt, buyer_uid),
                        )
                        if _table_exists(cur, "banka_user_notices"):
                            cur.execute(
                                "INSERT INTO banka_user_notices (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label) VALUES (%s, 'COINS_GRANTED', %s, %s, '', 'Telegram Stars');",
                                (buyer_uid, f"+{c_amt} монет", f"Зачислено за оплату ⭐ {stars_amount} Stars."),
                            )
                        summary_text = f"🪙 На баланс @{buyer_login} начислено +{c_amt} монет!"
                    elif kind == "BACKGROUND":
                        bg_id = int(target_bg_id or 0)
                        if bg_id > 0:
                            if _table_exists(cur, "banka_user_backgrounds"):
                                cur.execute(
                                    """
                                    INSERT INTO banka_user_backgrounds (owner_uid, bg_uid, granted_via)
                                    VALUES (%s, %s, 'STARS')
                                    ON CONFLICT (owner_uid, bg_uid) DO NOTHING;
                                    """,
                                    (buyer_uid, bg_id),
                                )
                            if _table_exists(cur, "banka_shop_backgrounds"):
                                cur.execute(
                                    """
                                    UPDATE banka_shop_backgrounds
                                    SET sold_count = COALESCE(sold_count, 0) + 1,
                                        earned_stars = COALESCE(earned_stars, 0) + %s
                                    WHERE bg_uid = %s;
                                    """,
                                    (int(stars_amount), bg_id),
                                )
                            if _table_exists(cur, "banka_user_notices"):
                                cur.execute(
                                    "INSERT INTO banka_user_notices (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label) VALUES (%s, 'COINS_GRANTED', 'Новый фон профиля!', %s, '', 'Telegram Stars');",
                                    (buyer_uid, f"Фон разблокирован за ⭐ {stars_amount} Stars."),
                                )
                        summary_text = f"🖼 Фон профиля открыт для @{buyer_login}!"
        conn.commit()
        return already_done, should_refund, summary_text

    already_done, should_refund, summary_text = with_db(_apply_payment)
    if already_done:
        return

    if should_refund:
        try:
            bot.refund_star_payment(tg_id, charge_id)
            with_db(lambda conn: (conn.cursor().execute("UPDATE banka_stars_orders SET status_tag = 'REFUNDED' WHERE order_uid = %s;", (oid,)), conn.commit()))
            bot.send_message(message.chat.id, "↩️ Заказ уже был отменён ранее — звёзды автоматически возвращены на ваш баланс Telegram.")
        except Exception as e:
            bot.send_message(message.chat.id, f"⚠️ Заказ был отменён. Ошибка автовозврата: {e}")
        mark_invoices_cleaned(oid, except_delete=False)
        return

    mark_invoices_cleaned(oid, except_delete=False)
    bot.send_message(message.chat.id, summary_text)


def cancelled_invoice_watcher():
    while True:
        try:
            if _db_initialized:
                def _poll(conn):
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT i.order_uid, i.chat_id, i.message_id
                            FROM banka_stars_invoices i
                            JOIN banka_stars_orders o ON o.order_uid = i.order_uid
                            WHERE COALESCE(i.cleaned_flag, FALSE) = FALSE
                              AND o.status_tag <> 'PENDING'
                            LIMIT 25;
                            """
                        )
                        return cur.fetchall()

                rows = with_db(_poll)
                for oid, cid, mid in rows:
                    try:
                        bot.delete_message(int(cid), int(mid))
                    except Exception:
                        pass
                    with_db(lambda conn: (
                        conn.cursor().execute(
                            "UPDATE banka_stars_invoices SET cleaned_flag = TRUE WHERE order_uid = %s AND chat_id = %s AND message_id = %s;",
                            (int(oid), int(cid), int(mid)),
                        ),
                        conn.commit(),
                    ))
        except Exception:
            pass
        time.sleep(2.5)


# ============================================================
# HTML-ИНТЕРФЕЙСЫ (БЕЗОПАСНЫЙ DOM + АВТОПОВТОР ЗАПРОСОВ)
# ============================================================
PUBLIC_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GDZgo Bot — Портал</title>
<style>
  :root { --bg:#0b0f17; --card:#131a26; --bd:#243044; --txt:#e8eef8; --muted:#93a4bd; --acc:#4f8cff; --ok:#22c55e; --err:#ef4444; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--txt); min-height:100vh; display:flex; align-items:center; justify-content:center; padding:20px; }
  .card { width:100%; max-width:420px; background:var(--card); border:1px solid var(--bd); border-radius:16px; padding:24px; box-shadow:0 20px 60px rgba(0,0,0,.45); }
  h1 { margin:0 0 6px; font-size:22px; }
  p.sub { margin:0 0 18px; color:var(--muted); font-size:14px; }
  .tabs { display:flex; gap:8px; margin-bottom:16px; }
  .tab { flex:1; padding:10px; border-radius:10px; border:1px solid var(--bd); background:transparent; color:var(--muted); cursor:pointer; font-weight:600; }
  .tab.active { background:var(--acc); border-color:var(--acc); color:#fff; }
  label { display:block; font-size:13px; color:var(--muted); margin:10px 0 6px; }
  input { width:100%; padding:11px 12px; border-radius:10px; border:1px solid var(--bd); background:#0c111b; color:var(--txt); font-size:15px; outline:none; }
  input:focus { border-color:var(--acc); }
  button.primary { width:100%; margin-top:16px; padding:12px; border-radius:10px; border:none; background:var(--acc); color:#fff; font-size:15px; font-weight:600; cursor:pointer; }
  .msg { margin-top:14px; padding:12px; border-radius:10px; font-size:14px; display:none; }
  .msg.err { display:block; background:rgba(239,68,68,.15); border:1px solid rgba(239,68,68,.4); color:#fca5a5; }
  .msg.ok { display:block; background:rgba(34,197,94,.15); border:1px solid rgba(34,197,94,.4); color:#86efac; }
  .link-row { margin-top:14px; text-align:center; font-size:13px; color:var(--muted); }
  a { color:var(--acc); text-decoration:none; }
</style>
</head>
<body>
<div class="card">
  <h1>GDZgo Bot Portal</h1>
  <p class="sub">Первый зарегистрированный аккаунт становится Супер-администратором (логин и пароль также можно обновить в приложении GDZgo в Спец.действиях).</p>
  <div class="tabs">
    <button class="tab active" id="tabLogin" type="button">Вход</button>
    <button class="tab" id="tabReg" type="button">Регистрация</button>
  </div>
  <form id="authForm">
    <label for="username">Логин</label>
    <input id="username" autocomplete="username" required minlength="3" maxlength="50">
    <label for="password">Пароль</label>
    <input id="password" type="password" autocomplete="current-password" required minlength="4" maxlength="100">
    <button class="primary" id="submitBtn" type="submit">Войти</button>
  </form>
  <div id="statusBox" class="msg"></div>
  <div id="goAdminWrap" class="link-row" style="display:none;">
    <a href="/admin">Открыть панель управления (/admin) →</a>
  </div>
</div>
<script>
let mode = "login";
const tabLogin = document.getElementById("tabLogin");
const tabReg = document.getElementById("tabReg");
const submitBtn = document.getElementById("submitBtn");
const statusBox = document.getElementById("statusBox");
const goAdminWrap = document.getElementById("goAdminWrap");

function setMode(m) {
  mode = m;
  tabLogin.classList.toggle("active", m === "login");
  tabReg.classList.toggle("active", m === "register");
  submitBtn.textContent = m === "login" ? "Войти" : "Зарегистрироваться";
  statusBox.className = "msg";
  statusBox.textContent = "";
}
tabLogin.onclick = () => setMode("login");
tabReg.onclick = () => setMode("register");

async function safePost(url, payload) {
  for (let i = 0; i < 2; i++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const txt = await res.text();
      let data = {};
      try { data = JSON.parse(txt); } catch (_) {}
      if ((res.status === 502 || res.status === 503 || res.status === 504) && i === 0) {
        await new Promise(r => setTimeout(r, 800));
        continue;
      }
      return { ok: res.ok, status: res.status, data, raw: txt };
    } catch (e) {
      if (i === 0) {
        await new Promise(r => setTimeout(r, 800));
        continue;
      }
      throw e;
    }
  }
}

document.getElementById("authForm").onsubmit = async (e) => {
  e.preventDefault();
  statusBox.className = "msg";
  statusBox.textContent = "Подождите...";
  goAdminWrap.style.display = "none";
  const u = document.getElementById("username").value.trim();
  const p = document.getElementById("password").value;
  try {
    const r = await safePost(mode === "login" ? "/api/auth/login" : "/api/auth/register", { username: u, password: p });
    if (!r.ok) {
      statusBox.className = "msg err";
      statusBox.textContent = r.data.error || ("Ошибка сервера HTTP " + r.status);
      return;
    }
    if (r.data.role === "USER") {
      statusBox.className = "msg err";
      statusBox.textContent = "Вы не админ и не модератор. Обратитесь к супер-администратору для выдачи доступа.";
      return;
    }
    statusBox.className = "msg ok";
    statusBox.textContent = "Доступ подтверждён (" + r.data.role + "). Перейдите в /admin и войдите с этим аккаунтом.";
    goAdminWrap.style.display = "block";
  } catch (err) {
    statusBox.className = "msg err";
    statusBox.textContent = "Сервер не отвечает (" + (err.message || "таймаут") + "). Повторите через пару секунд.";
  }
};
</script>
</body>
</html>
"""

ADMIN_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GDZgo Bot — /admin</title>
<style>
  :root { --bg:#0a0e16; --panel:#121926; --panel2:#182132; --bd:#243045; --txt:#ebf1fa; --muted:#93a4be; --acc:#4f8cff; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--txt); }
  .wrap { max-width:1280px; margin:0 auto; padding:18px; }
  .topbar { display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:12px; background:var(--panel); border:1px solid var(--bd); border-radius:14px; padding:14px 18px; margin-bottom:16px; }
  .brand { font-size:18px; font-weight:700; display:flex; align-items:center; gap:10px; }
  .badge { font-size:12px; padding:3px 9px; border-radius:999px; background:rgba(79,140,255,.18); color:#8cb4ff; border:1px solid rgba(79,140,255,.35); }
  .nav { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:16px; }
  .nav button { padding:9px 14px; border-radius:10px; border:1px solid var(--bd); background:var(--panel); color:var(--muted); cursor:pointer; font-weight:600; }
  .nav button.active { background:var(--acc); border-color:var(--acc); color:#fff; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:12px; margin-bottom:18px; }
  .stat { background:var(--panel); border:1px solid var(--bd); border-radius:14px; padding:16px; }
  .stat .lbl { font-size:13px; color:var(--muted); }
  .stat .val { font-size:24px; font-weight:700; margin-top:6px; }
  .card { background:var(--panel); border:1px solid var(--bd); border-radius:14px; padding:16px; margin-bottom:16px; }
  .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  input, select, textarea { width:100%; padding:10px 12px; border-radius:10px; border:1px solid var(--bd); background:#0c111b; color:var(--txt); font-size:14px; outline:none; }
  input:focus, textarea:focus, select:focus { border-color:var(--acc); }
  button.btn { padding:9px 14px; border-radius:10px; border:1px solid var(--bd); background:var(--panel2); color:var(--txt); cursor:pointer; font-weight:600; font-size:13px; }
  button.btn.primary { background:var(--acc); border-color:var(--acc); color:#fff; }
  button.btn.danger { background:rgba(239,68,68,.16); border-color:rgba(239,68,68,.4); color:#fca5a5; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:10px 8px; border-bottom:1px solid var(--bd); vertical-align:top; }
  th { color:var(--muted); font-weight:600; }
  tr.click { cursor:pointer; }
  tr.click:hover { background:rgba(79,140,255,.08); }
  .login-wrap { max-width:410px; margin:70px auto; background:var(--panel); border:1px solid var(--bd); border-radius:16px; padding:24px; }
  .msg { margin-top:12px; padding:10px 12px; border-radius:10px; font-size:13px; }
  .msg.err { background:rgba(239,68,68,.16); border:1px solid rgba(239,68,68,.4); color:#fca5a5; }
  .msg.ok { background:rgba(34,197,94,.16); border:1px solid rgba(34,197,94,.4); color:#86efac; }
  .modal-bg { position:fixed; inset:0; background:rgba(0,0,0,.72); display:none; align-items:center; justify-content:center; padding:16px; z-index:50; }
  .modal-bg.open { display:flex; }
  .modal { width:100%; max-width:960px; max-height:90vh; overflow-y:auto; background:var(--panel); border:1px solid var(--bd); border-radius:16px; padding:20px; }
  .chat-split { display:grid; grid-template-columns:290px 1fr; gap:12px; min-height:380px; }
  @media (max-width: 760px) { .chat-split { grid-template-columns:1fr; } }
  .chat-list { border:1px solid var(--bd); border-radius:12px; background:#0c111b; overflow-y:auto; max-height:460px; }
  .chat-room-item { padding:10px 12px; border-bottom:1px solid var(--bd); cursor:pointer; }
  .chat-room-item:hover, .chat-room-item.active { background:rgba(79,140,255,.15); }
  .chat-feed { border:1px solid var(--bd); border-radius:12px; background:#0c111b; padding:12px; overflow-y:auto; max-height:460px; display:flex; flex-direction:column; gap:10px; }
  .bubble { border:1px solid var(--bd); background:var(--panel2); border-radius:12px; padding:9px 12px; }
  .bubble.deleted { opacity:.65; border-style:dashed; }
  .bubble-meta { font-size:11px; color:var(--muted); margin-bottom:4px; display:flex; justify-content:space-between; gap:8px; flex-wrap:wrap; }
  .bubble-quote { font-size:12px; color:#a5b4fc; border-left:2px solid var(--acc); padding-left:8px; margin-bottom:5px; }
  .bubble-text { font-size:14px; white-space:pre-wrap; word-break:break-word; }
  .preview-img { max-width:260px; max-height:200px; border-radius:10px; border:1px solid var(--bd); margin-top:8px; display:block; }
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; border:1px solid var(--bd); }
  .pill.paid { color:#86efac; border-color:rgba(34,197,94,.45); background:rgba(34,197,94,.12); }
  .pill.pending { color:#fde047; border-color:rgba(245,158,11,.45); background:rgba(245,158,11,.12); }
  .pill.bad { color:#fca5a5; border-color:rgba(239,68,68,.45); background:rgba(239,68,68,.12); }
</style>
</head>
<body>

<div id="loginView" class="login-wrap">
  <h2 style="margin:0 0 6px;">Вход в /admin</h2>
  <p style="margin:0 0 14px;color:var(--muted);font-size:13px;">Авторизация администратора или модератора. Регистрация доступна на <a href="/" style="color:var(--acc);">главной странице</a>.</p>
  <form id="adminLoginForm">
    <label style="font-size:13px;color:var(--muted);display:block;margin:8px 0 4px;">Логин</label>
    <input id="loginUser" autocomplete="username" required>
    <label style="font-size:13px;color:var(--muted);display:block;margin:10px 0 4px;">Пароль</label>
    <input id="loginPass" type="password" autocomplete="current-password" required>
    <button class="btn primary" type="submit" style="width:100%;margin-top:14px;padding:11px;">Войти</button>
  </form>
  <div id="loginErr" class="msg err" style="display:none;"></div>
</div>

<div id="appView" class="wrap" style="display:none;">
  <div class="topbar">
    <div class="brand">
      <span>⭐ GDZgo Admin</span>
      <span id="meBadge" class="badge"></span>
    </div>
    <div class="row">
      <button class="btn" id="refreshAllBtn" type="button">🔄 Обновить</button>
      <button class="btn danger" id="logoutBtn" type="button">Выйти</button>
    </div>
  </div>

  <div class="nav" id="mainNav"></div>
  <div id="tabContent"></div>
</div>

<div class="modal-bg" id="userModalBg">
  <div class="modal">
    <div class="row" style="justify-content:space-between;margin-bottom:12px;">
      <div>
        <h3 id="modalTitle" style="margin:0;">Профиль</h3>
        <div id="modalSub" style="font-size:13px;color:var(--muted);margin-top:2px;"></div>
      </div>
      <button class="btn" id="closeModalBtn" type="button">✕ Закрыть</button>
    </div>
    <div class="nav" id="modalTabs"></div>
    <div id="modalBody"></div>
  </div>
</div>

<script>
let authToken = "";
let meData = null;
let currentTab = "dashboard";
let broadcastPhotoDataUrl = "";
let dmPhotoDataUrl = "";

function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "style") node.setAttribute("style", v);
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2).toLowerCase(), v);
      else if (v !== null && v !== undefined) node.setAttribute(k, String(v));
    }
  }
  if (children !== undefined && children !== null) {
    const arr = Array.isArray(children) ? children : [children];
    for (const c of arr) {
      if (c === null || c === undefined) continue;
      if (typeof c === "string" || typeof c === "number") node.appendChild(document.createTextNode(String(c)));
      else node.appendChild(c);
    }
  }
  return node;
}

function hasPerm(perm) {
  if (!meData) return false;
  if (meData.role === "SUPER_ADMIN") return true;
  return meData.role === "MODERATOR" && Array.isArray(meData.permissions) && meData.permissions.includes(perm);
}

async function apiFetch(url, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  if (authToken) headers["Authorization"] = "Bearer " + authToken;
  if (opts.body && !(opts.body instanceof FormData) && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const res = await fetch(url, Object.assign({}, opts, { headers }));
      const txt = await res.text();
      let data = {};
      try { data = JSON.parse(txt); } catch (_) {}
      if ((res.status === 502 || res.status === 503 || res.status === 504) && attempt === 0) {
        await new Promise(r => setTimeout(r, 700));
        continue;
      }
      if (!res.ok) throw new Error(data.error || ("HTTP " + res.status));
      return data;
    } catch (e) {
      if (attempt === 0 && (e instanceof TypeError)) {
        await new Promise(r => setTimeout(r, 700));
        continue;
      }
      throw e;
    }
  }
}

function readImageFile(fileInput, maxBytes, onOk, onErr) {
  const file = fileInput.files && fileInput.files[0];
  if (!file) { onOk(""); return; }
  const allowed = ["image/png", "image/jpeg", "image/webp", "image/gif"];
  if (!allowed.includes(file.type)) { onErr("Допустимы только PNG, JPEG, WEBP или GIF"); return; }
  if (file.size > maxBytes) { onErr("Размер фото больше 5.5 МБ"); return; }
  const reader = new FileReader();
  reader.onload = () => onOk(typeof reader.result === "string" ? reader.result : "");
  reader.onerror = () => onErr("Не удалось прочитать файл");
  reader.readAsDataURL(file);
}

function isSafeImageDataUrl(val) {
  if (typeof val !== "string" || !val) return false;
  return /^data:image\/(png|jpeg|jpg|webp|gif);base64,[A-Za-z0-9+/=\r\n]+$/i.test(val.trim());
}

function isSafeRawBase64(val) {
  if (typeof val !== "string" || !val) return false;
  return /^[A-Za-z0-9+/=\r\n]+$/.test(val.trim());
}

document.getElementById("adminLoginForm").onsubmit = async (e) => {
  e.preventDefault();
  const errBox = document.getElementById("loginErr");
  errBox.style.display = "none";
  try {
    const data = await apiFetch("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({
        username: document.getElementById("loginUser").value.trim(),
        password: document.getElementById("loginPass").value
      })
    });
    if (data.role === "USER") {
      errBox.textContent = "Вы не админ и не модератор";
      errBox.style.display = "block";
      return;
    }
    authToken = data.token;
    meData = data;
    document.getElementById("loginView").style.display = "none";
    document.getElementById("appView").style.display = "block";
    document.getElementById("meBadge").textContent = meData.username + " (" + meData.role + ")";
    renderNav();
    loadActiveTab();
  } catch (err) {
    errBox.textContent = err.message || "Ошибка подключения к серверу";
    errBox.style.display = "block";
  }
};

document.getElementById("logoutBtn").onclick = async () => {
  try { await apiFetch("/api/auth/logout", { method: "POST" }); } catch (_) {}
  authToken = "";
  meData = null;
  document.getElementById("appView").style.display = "none";
  document.getElementById("loginView").style.display = "block";
};

document.getElementById("refreshAllBtn").onclick = () => loadActiveTab();
document.getElementById("closeModalBtn").onclick = () => document.getElementById("userModalBg").classList.remove("open");

function renderNav() {
  const nav = document.getElementById("mainNav");
  nav.textContent = "";
  const tabs = [];
  if (hasPerm("view_dashboard")) tabs.push(["dashboard", "📊 Статистика"]);
  if (hasPerm("view_users")) tabs.push(["bot_users", "🤖 Пользователи бота"]);
  if (hasPerm("view_users") || hasPerm("view_chats")) tabs.push(["gdz_users", "🎓 Все аккаунты GDZgo"]);
  if (hasPerm("broadcast")) tabs.push(["broadcast", "📢 Рассылка"]);
  if (meData && meData.role === "SUPER_ADMIN") tabs.push(["moderators", "🛡️ Модераторы"]);

  if (tabs.length === 0) {
    document.getElementById("tabContent").textContent = "У вашей роли нет доступных разделов.";
    return;
  }
  if (!tabs.some(t => t[0] === currentTab)) currentTab = tabs[0][0];

  for (const [id, label] of tabs) {
    const b = el("button", { class: id === currentTab ? "active" : "", type: "button" }, label);
    b.onclick = () => { currentTab = id; renderNav(); loadActiveTab(); };
    nav.appendChild(b);
  }
}

async function loadActiveTab() {
  const root = document.getElementById("tabContent");
  root.textContent = "Загрузка...";
  try {
    if (currentTab === "dashboard") await renderDashboard(root);
    else if (currentTab === "bot_users") await renderBotUsers(root);
    else if (currentTab === "gdz_users") await renderGdzUsers(root);
    else if (currentTab === "broadcast") await renderBroadcast(root);
    else if (currentTab === "moderators") await renderModerators(root);
  } catch (err) {
    root.textContent = "";
    root.appendChild(el("div", { class: "msg err" }, "Ошибка: " + err.message));
  }
}

async function renderDashboard(root) {
  const d = await apiFetch("/api/dashboard");
  root.textContent = "";
  const grid = el("div", { class: "grid" }, [
    statCard("Доход сегодня (МСК)", "⭐ " + d.stars_today + " (" + d.orders_today + " шт.)"),
    statCard("Доход за 7 дней", "⭐ " + d.stars_week + " (" + d.orders_week + " шт.)"),
    statCard("Доход за 30 дней", "⭐ " + d.stars_month + " (" + d.orders_month + " шт.)"),
    statCard("Доход за всё время", "⭐ " + d.stars_all + " (" + d.orders_all + " шт.)"),
    statCard("Пользователей бота", String(d.bot_users_count)),
    statCard("Аккаунтов GDZgo", String(d.gdz_users_count)),
    statCard("Чатов / сообщений", d.chat_rooms_count + " / " + d.chat_messages_count),
    statCard("Ожидают / Отменены", d.pending_orders + " / " + d.cancelled_orders)
  ]);
  root.appendChild(grid);

  const card = el("div", { class: "card" });
  card.appendChild(el("h3", { style: "margin:0 0 12px;" }, "Последние оплаты Telegram Stars"));
  const tbl = el("table");
  tbl.appendChild(el("thead", null, el("tr", null, [
    el("th", null, "Время (МСК)"), el("th", null, "GDZgo логин"), el("th", null, "Товар"), el("th", null, "Звёзды"), el("th", null, "Статус")
  ])));
  const tb = el("tbody");
  for (const o of (d.recent_orders || [])) {
    const tr = el("tr", { class: "click" }, [
      el("td", null, o.created_msk),
      el("td", null, o.buyer_login || ("UID #" + o.buyer_uid)),
      el("td", null, o.item_label),
      el("td", null, "⭐ " + o.stars_amount),
      el("td", null, statusPill(o.status_tag))
    ]);
    tr.onclick = () => openUserModal({ tg_user_id: o.telegram_user_id || 0, gdz_user_id: o.buyer_uid, title: o.buyer_login || ("UID #" + o.buyer_uid) });
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  card.appendChild(tbl);
  root.appendChild(card);
}

function statCard(lbl, val) {
  return el("div", { class: "stat" }, [
    el("div", { class: "lbl" }, lbl),
    el("div", { class: "val" }, val)
  ]);
}

function statusPill(st) {
  const s = String(st || "");
  const cls = s === "PAID" ? "pill paid" : (s === "PENDING" ? "pill pending" : "pill bad");
  return el("span", { class: cls }, s);
}

async function renderBotUsers(root, query = "") {
  const data = await apiFetch("/api/users" + (query ? ("?q=" + encodeURIComponent(query)) : ""));
  root.textContent = "";
  const searchRow = el("div", { class: "card row" });
  const inp = el("input", { placeholder: "Поиск по @username, имени, Telegram ID или логину GDZgo...", value: query, style: "flex:1;min-width:220px;" });
  const btn = el("button", { class: "btn primary", type: "button" }, "Найти");
  btn.onclick = () => renderBotUsers(root, inp.value.trim());
  inp.onkeydown = (e) => { if (e.key === "Enter") renderBotUsers(root, inp.value.trim()); };
  searchRow.appendChild(inp);
  searchRow.appendChild(btn);
  root.appendChild(searchRow);

  const card = el("div", { class: "card" });
  card.appendChild(el("div", { style: "color:var(--muted);font-size:13px;margin-bottom:10px;" }, "Кликните по строке пользователя, чтобы открыть его профиль, чаты GDZgo и покупки Stars."));
  const tbl = el("table");
  tbl.appendChild(el("thead", null, el("tr", null, [
    el("th", null, "Telegram"), el("th", null, "Аккаунт GDZgo"), el("th", null, "Потрачено ⭐"), el("th", null, "Покупок"), el("th", null, "Активность (МСК)")
  ])));
  const tb = el("tbody");
  for (const u of (data.users || [])) {
    const tgTitle = [u.tg_first_name, u.tg_last_name].filter(Boolean).join(" ") || ("TG #" + u.tg_user_id);
    const gdzLabel = u.gdz_user_id > 0 ? ((u.gdz_fullname || u.gdz_login || ("#" + u.gdz_user_id)) + " (@" + u.gdz_login + ")") : "Не привязан";
    const tr = el("tr", { class: "click" }, [
      el("td", null, tgTitle + (u.tg_username ? " (@" + u.tg_username + ")" : "") + " [" + u.tg_user_id + "]"),
      el("td", null, gdzLabel),
      el("td", null, "⭐ " + u.spent_stars),
      el("td", null, String(u.paid_orders_count)),
      el("td", null, u.last_seen_msk)
    ]);
    tr.onclick = () => openUserModal({
      tg_user_id: u.tg_user_id,
      gdz_user_id: u.gdz_user_id,
      title: tgTitle,
      subtitle: "TG @" + (u.tg_username || "—") + " • GDZgo: " + gdzLabel
    });
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  card.appendChild(tbl);
  root.appendChild(card);
}

async function renderGdzUsers(root, query = "") {
  const data = await apiFetch("/api/gdz-users" + (query ? ("?q=" + encodeURIComponent(query)) : ""));
  root.textContent = "";
  const searchRow = el("div", { class: "card row" });
  const inp = el("input", { placeholder: "Поиск по логину, ФИО, ID или классу GDZgo...", value: query, style: "flex:1;min-width:220px;" });
  const btn = el("button", { class: "btn primary", type: "button" }, "Найти");
  btn.onclick = () => renderGdzUsers(root, inp.value.trim());
  inp.onkeydown = (e) => { if (e.key === "Enter") renderGdzUsers(root, inp.value.trim()); };
  searchRow.appendChild(inp);
  searchRow.appendChild(btn);
  root.appendChild(searchRow);

  const card = el("div", { class: "card" });
  card.appendChild(el("div", { style: "color:var(--muted);font-size:13px;margin-bottom:10px;" }, "Все аккаунты приложения GDZgo — нажмите на любой профиль для просмотра переписок и покупок."));
  const tbl = el("table");
  tbl.appendChild(el("thead", null, el("tr", null, [
    el("th", null, "ID / Логин"), el("th", null, "Имя"), el("th", null, "Роль / VIP"), el("th", null, "Чатов"), el("th", null, "Потрачено ⭐")
  ])));
  const tb = el("tbody");
  for (const u of (data.users || [])) {
    const tr = el("tr", { class: "click" }, [
      el("td", null, "#" + u.rec_uid + " @" + u.login),
      el("td", null, (u.full_name || u.login) + (u.cohort_code ? (" (" + u.cohort_code + ")") : "")),
      el("td", null, u.rank_badge + (u.is_vip ? " • 👑 VIP" : "")),
      el("td", null, String(u.chats_count)),
      el("td", null, "⭐ " + u.spent_stars)
    ]);
    tr.onclick = () => openUserModal({
      tg_user_id: u.tg_user_id || 0,
      gdz_user_id: u.rec_uid,
      title: (u.full_name || u.login) + " (@" + u.login + ")",
      subtitle: "GDZgo ID #" + u.rec_uid + " • Монеты: " + u.coins + (u.tg_user_id ? (" • TG #" + u.tg_user_id) : " • TG не привязан")
    });
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  card.appendChild(tbl);
  root.appendChild(card);
}

async function openUserModal(info) {
  const bg = document.getElementById("userModalBg");
  document.getElementById("modalTitle").textContent = info.title || "Профиль";
  document.getElementById("modalSub").textContent = info.subtitle || ("GDZgo #" + (info.gdz_user_id || 0) + " • TG #" + (info.tg_user_id || 0));
  bg.classList.add("open");

  const tabsEl = document.getElementById("modalTabs");
  const bodyEl = document.getElementById("modalBody");
  let active = hasPerm("view_chats") ? "chats" : (hasPerm("view_users") ? "orders" : "dm");

  function drawModalTabs() {
    tabsEl.textContent = "";
    const list = [];
    if (hasPerm("view_chats")) list.push(["chats", "💬 Чаты и переписки GDZgo"]);
    if (hasPerm("view_users")) list.push(["orders", "⭐ Покупки Stars"]);
    if (hasPerm("send_dm") && info.tg_user_id > 0) list.push(["dm", "✉️ Написать в бота (ЛС)"]);
    for (const [id, lbl] of list) {
      const b = el("button", { class: id === active ? "active" : "", type: "button" }, lbl);
      b.onclick = () => { active = id; drawModalTabs(); loadModalTab(); };
      tabsEl.appendChild(b);
    }
  }

  async function loadModalTab() {
    bodyEl.textContent = "Загрузка...";
    try {
      if (active === "chats") await renderUserChatsTab(bodyEl, info);
      else if (active === "orders") await renderUserOrdersTab(bodyEl, info);
      else if (active === "dm") await renderUserDmTab(bodyEl, info);
    } catch (err) {
      bodyEl.textContent = "";
      bodyEl.appendChild(el("div", { class: "msg err" }, err.message));
    }
  }

  drawModalTabs();
  loadModalTab();
}

async function renderUserChatsTab(container, info) {
  let res;
  if (info.gdz_user_id > 0) {
    res = await apiFetch("/api/gdz-users/" + info.gdz_user_id + "/chats");
  } else if (info.tg_user_id > 0) {
    res = await apiFetch("/api/users/" + info.tg_user_id + "/chats");
  } else {
    container.textContent = "У пользователя нет привязанного аккаунта GDZgo.";
    return;
  }
  const rooms = res.rooms || [];
  container.textContent = "";
  if (rooms.length === 0) {
    container.appendChild(el("div", { class: "card" }, "У этого пользователя ещё нет чатов в GDZgo."));
    return;
  }

  const split = el("div", { class: "chat-split" });
  const listEl = el("div", { class: "chat-list" });
  const feedEl = el("div", { class: "chat-feed" }, "Выберите чат слева, чтобы загрузить переписку.");

  rooms.forEach((r, idx) => {
    const item = el("div", { class: "chat-room-item" + (idx === 0 ? " active" : "") }, [
      el("div", { style: "font-weight:600;font-size:14px;" }, r.display_title),
      el("div", { style: "font-size:12px;color:var(--muted);margin-top:3px;" }, r.kind_tag + " • Сообщений: " + r.messages_count + " • " + r.updated_msk)
    ]);
    item.onclick = () => {
      for (const ch of listEl.children) ch.classList.remove("active");
      item.classList.add("active");
      loadRoomTranscript(feedEl, r.room_uid);
    };
    listEl.appendChild(item);
  });

  split.appendChild(listEl);
  split.appendChild(feedEl);
  container.appendChild(split);
  loadRoomTranscript(feedEl, rooms[0].room_uid);
}

async function loadRoomTranscript(feedEl, roomUid) {
  feedEl.textContent = "Загрузка сообщений...";
  try {
    const data = await apiFetch("/api/chats/" + roomUid + "/messages");
    const msgs = data.messages || [];
    feedEl.textContent = "";
    if (msgs.length === 0) {
      feedEl.textContent = "В этом чате нет сообщений.";
      return;
    }
    for (const m of msgs) {
      const bubble = el("div", { class: "bubble" + (m.is_deleted ? " deleted" : "") });
      bubble.appendChild(el("div", { class: "bubble-meta" }, [
        el("span", null, (m.sender_name || "Пользователь") + " (@" + (m.sender_login || "—") + ")"),
        el("span", null, m.created_msk + (m.is_edited ? " • ред." : "") + (m.is_deleted ? " • УДАЛЕНО" : ""))
      ]));
      if (m.forwarded_from_name) {
        bubble.appendChild(el("div", { class: "bubble-quote" }, "Переслано от: " + m.forwarded_from_name));
      }
      if (m.reply_preview) {
        bubble.appendChild(el("div", { class: "bubble-quote" }, (m.reply_sender_name ? (m.reply_sender_name + ": ") : "") + m.reply_preview));
      }

      let bodyTxt = m.text_body || "";
      if (m.kind_tag === "VOICE") bodyTxt = "🎤 Голосовое (" + m.voice_sec + " сек.)" + (bodyTxt ? ("\n" + bodyTxt) : "");
      else if (m.kind_tag === "SHARED_MATERIAL") bodyTxt = "📚 Материал: " + (m.shared_mat_title || "—") + (bodyTxt ? ("\n" + bodyTxt) : "");
      else if (m.kind_tag === "SHARED_AI_CHAT") bodyTxt = "🤖 ИИ-чат: " + (m.shared_ai_title || "—") + (bodyTxt ? ("\n" + bodyTxt) : "");
      else if (m.kind_tag === "IMAGE" && !bodyTxt) bodyTxt = "🖼 Фото" + (m.media_name ? (" (" + m.media_name + ")") : "");

      if (bodyTxt) bubble.appendChild(el("div", { class: "bubble-text" }, bodyTxt));

      if (m.has_media) {
        const mediaHolder = el("div", { style: "margin-top:6px;" });
        const showBtn = el("button", { class: "btn", type: "button" }, "🖼 Показать вложенное фото");
        showBtn.onclick = async () => {
          showBtn.disabled = true;
          showBtn.textContent = "Загрузка...";
          try {
            const mr = await apiFetch("/api/chats/messages/" + m.msg_uid + "/media");
            if (isSafeRawBase64(mr.media_b64)) {
              const img = el("img", { class: "preview-img", src: "data:image/jpeg;base64," + mr.media_b64.trim(), alt: "Фото из чата" });
              mediaHolder.textContent = "";
              mediaHolder.appendChild(img);
            } else {
              mediaHolder.textContent = "Некорректный формат медиа.";
            }
          } catch (e) {
            showBtn.textContent = "Ошибка загрузки фото";
          }
        };
        mediaHolder.appendChild(showBtn);
        bubble.appendChild(mediaHolder);
      }
      feedEl.appendChild(bubble);
    }
    feedEl.scrollTop = feedEl.scrollHeight;
  } catch (err) {
    feedEl.textContent = "Ошибка загрузки сообщений: " + err.message;
  }
}

async function renderUserOrdersTab(container, info) {
  const qs = info.tg_user_id > 0 ? ("/api/users/" + info.tg_user_id) : ("/api/gdz-users/" + info.gdz_user_id + "/detail");
  const d = await apiFetch(qs);
  container.textContent = "";
  const orders = d.orders || [];
  if (orders.length === 0) {
    container.appendChild(el("div", { class: "card" }, "Покупок за Stars пока нет."));
    return;
  }
  const tbl = el("table");
  tbl.appendChild(el("thead", null, el("tr", null, [
    el("th", null, "Время (МСК)"), el("th", null, "Код"), el("th", null, "Товар"), el("th", null, "Звёзды"), el("th", null, "Статус")
  ])));
  const tb = el("tbody");
  for (const o of orders) {
    tb.appendChild(el("tr", null, [
      el("td", null, o.created_msk),
      el("td", null, o.pay_code),
      el("td", null, o.item_label),
      el("td", null, "⭐ " + o.stars_amount),
      el("td", null, statusPill(o.status_tag))
    ]));
  }
  tbl.appendChild(tb);
  container.appendChild(tbl);
}

async function renderUserDmTab(container, info) {
  const d = await apiFetch("/api/users/" + info.tg_user_id);
  container.textContent = "";
  dmPhotoDataUrl = "";

  const formCard = el("div", { class: "card" });
  const ta = el("textarea", { rows: 4, placeholder: "Текст личного сообщения (или подпись к фото)..." });
  const fileInp = el("input", { type: "file", accept: "image/png,image/jpeg,image/webp,image/gif", style: "margin-top:8px;" });
  const previewWrap = el("div");
  const clearBtn = el("button", { class: "btn", type: "button", style: "display:none;margin-top:6px;" }, "Убрать фото");
  const statusEl = el("div");

  fileInp.onchange = () => {
    statusEl.textContent = "";
    readImageFile(fileInp, 5_500_000, (url) => {
      dmPhotoDataUrl = url;
      previewWrap.textContent = "";
      if (isSafeImageDataUrl(url)) {
        previewWrap.appendChild(el("img", { class: "preview-img", src: url, alt: "Превью" }));
        clearBtn.style.display = "inline-block";
      }
    }, (err) => {
      dmPhotoDataUrl = "";
      fileInp.value = "";
      statusEl.className = "msg err";
      statusEl.textContent = err;
    });
  };
  clearBtn.onclick = () => { dmPhotoDataUrl = ""; fileInp.value = ""; previewWrap.textContent = ""; clearBtn.style.display = "none"; };

  const sendBtn = el("button", { class: "btn primary", type: "button", style: "margin-top:10px;" }, "Отправить в Telegram");
  sendBtn.onclick = async () => {
    statusEl.textContent = "";
    const txt = ta.value.trim();
    if (!txt && !dmPhotoDataUrl) {
      statusEl.className = "msg err"; statusEl.textContent = "Введите текст или выберите фото"; return;
    }
    sendBtn.disabled = true;
    try {
      await apiFetch("/api/messages/dm", {
        method: "POST",
        body: JSON.stringify({ tg_user_id: info.tg_user_id, text: txt, photo_b64: dmPhotoDataUrl })
      });
      ta.value = "";
      clearBtn.click();
      statusEl.className = "msg ok";
      statusEl.textContent = "Сообщение отправлено!";
    } catch (e) {
      statusEl.className = "msg err";
      statusEl.textContent = e.message;
    } finally {
      sendBtn.disabled = false;
    }
  };

  formCard.appendChild(ta);
  formCard.appendChild(fileInp);
  formCard.appendChild(previewWrap);
  formCard.appendChild(clearBtn);
  formCard.appendChild(el("div", null, sendBtn));
  formCard.appendChild(statusEl);
  container.appendChild(formCard);

  if (d.messages && d.messages.length > 0) {
    const hist = el("div", { class: "card" });
    hist.appendChild(el("h4", { style: "margin:0 0 8px;" }, "История ЛС из админки"));
    for (const m of d.messages) {
      hist.appendChild(el("div", { style: "padding:6px 0;border-bottom:1px solid var(--bd);font-size:13px;" },
        m.created_msk + " • " + m.sender_admin + (m.has_photo ? " [📷 Фото]" : "") + ": " + (m.message_text || "(только фото)")
      ));
    }
    container.appendChild(hist);
  }
}

async function renderBroadcast(root) {
  root.textContent = "";
  broadcastPhotoDataUrl = "";
  const card = el("div", { class: "card" });
  card.appendChild(el("h3", { style: "margin:0 0 10px;" }, "📢 Массовая рассылка в Telegram-боте"));

  const aud = el("select", { style: "margin-bottom:10px;" }, [
    el("option", { value: "all" }, "Всем активным пользователям бота"),
    el("option", { value: "linked" }, "Только с привязанным аккаунтом GDZgo"),
    el("option", { value: "buyers" }, "Только покупателям Stars")
  ]);
  const ta = el("textarea", { rows: 5, placeholder: "Текст рассылки или рекламная подпись к фото (до 1024 симв. в подписи; длинный текст придёт вторым сообщением)..." });
  const fileInp = el("input", { type: "file", accept: "image/png,image/jpeg,image/webp,image/gif", style: "margin-top:10px;" });
  const previewWrap = el("div");
  const clearBtn = el("button", { class: "btn", type: "button", style: "display:none;margin-top:8px;" }, "✕ Удалить фото");
  const statusEl = el("div");

  fileInp.onchange = () => {
    statusEl.textContent = "";
    statusEl.className = "";
    readImageFile(fileInp, 5_500_000, (url) => {
      broadcastPhotoDataUrl = url;
      previewWrap.textContent = "";
      if (isSafeImageDataUrl(url)) {
        previewWrap.appendChild(el("img", { class: "preview-img", src: url, alt: "Превью рассылки" }));
        clearBtn.style.display = "inline-block";
      }
    }, (err) => {
      broadcastPhotoDataUrl = "";
      fileInp.value = "";
      statusEl.className = "msg err";
      statusEl.textContent = err;
    });
  };
  clearBtn.onclick = () => {
    broadcastPhotoDataUrl = "";
    fileInp.value = "";
    previewWrap.textContent = "";
    clearBtn.style.display = "none";
  };

  const sendBtn = el("button", { class: "btn primary", type: "button", style: "margin-top:12px;" }, "🚀 Запустить рассылку");
  sendBtn.onclick = async () => {
    statusEl.textContent = "";
    statusEl.className = "";
    const txt = ta.value.trim();
    if (!txt && !broadcastPhotoDataUrl) {
      statusEl.className = "msg err";
      statusEl.textContent = "Добавьте текст или фото для рассылки.";
      return;
    }
    sendBtn.disabled = true;
    sendBtn.textContent = "Отправка...";
    try {
      const res = await apiFetch("/api/messages/broadcast", {
        method: "POST",
        body: JSON.stringify({ audience: aud.value, text: txt, photo_b64: broadcastPhotoDataUrl })
      });
      statusEl.className = "msg ok";
      statusEl.textContent = "Готово! Отправлено: " + res.sent + ", ошибок/блокировок: " + res.failed + " из " + res.total;
      ta.value = "";
      clearBtn.click();
    } catch (e) {
      statusEl.className = "msg err";
      statusEl.textContent = "Ошибка: " + e.message;
    } finally {
      sendBtn.disabled = false;
      sendBtn.textContent = "🚀 Запустить рассылку";
    }
  };

  card.appendChild(aud);
  card.appendChild(ta);
  card.appendChild(fileInp);
  card.appendChild(previewWrap);
  card.appendChild(clearBtn);
  card.appendChild(el("div", null, sendBtn));
  card.appendChild(statusEl);
  root.appendChild(card);
}

async function renderModerators(root) {
  const data = await apiFetch("/api/moderators");
  root.textContent = "";
  const permsCatalog = data.all_permissions || [];
  const card = el("div", { class: "card" });
  card.appendChild(el("h3", { style: "margin:0 0 10px;" }, "🛡️ Управление пользователями портала и модераторами"));

  for (const a of (data.admins || [])) {
    const box = el("div", { style: "border:1px solid var(--bd);border-radius:12px;padding:12px;margin-bottom:10px;background:var(--panel2);" });
    const head = el("div", { class: "row", style: "justify-content:space-between;" }, [
      el("div", null, el("strong", null, a.username + " (" + a.role + ")")),
      el("div", { style: "font-size:12px;color:var(--muted);" }, "Создан: " + a.created_msk)
    ]);
    box.appendChild(head);

    if (a.role !== "SUPER_ADMIN") {
      const roleRow = el("div", { class: "row", style: "margin-top:10px;" });
      const btnToggle = el("button", { class: "btn " + (a.role === "MODERATOR" ? "danger" : "primary"), type: "button" },
        a.role === "MODERATOR" ? "Снять модератора (в USER)" : "Назначить MODERATOR"
      );
      btnToggle.onclick = async () => {
        await apiFetch("/api/moderators/role", {
          method: "POST",
          body: JSON.stringify({ admin_uid: a.admin_uid, role: a.role === "MODERATOR" ? "USER" : "MODERATOR" })
        });
        renderModerators(root);
      };
      roleRow.appendChild(btnToggle);
      box.appendChild(roleRow);

      if (a.role === "MODERATOR") {
        const pWrap = el("div", { style: "margin-top:10px;display:flex;flex-wrap:wrap;gap:10px;" });
        const checks = new Map();
        for (const p of permsCatalog) {
          const cb = el("input", { type: "checkbox", style: "width:auto;" });
          cb.checked = (a.permissions || []).includes(p.key);
          checks.set(p.key, cb);
          pWrap.appendChild(el("label", { style: "display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;" }, [cb, p.label]));
        }
        const saveP = el("button", { class: "btn primary", type: "button", style: "margin-top:8px;" }, "Сохранить права");
        saveP.onclick = async () => {
          const selected = [];
          for (const [k, cb] of checks.entries()) if (cb.checked) selected.push(k);
          await apiFetch("/api/moderators/perms", {
            method: "POST",
            body: JSON.stringify({ admin_uid: a.admin_uid, permissions: selected })
          });
          saveP.textContent = "✅ Сохранено";
          setTimeout(() => { saveP.textContent = "Сохранить права"; }, 1200);
        };
        box.appendChild(pWrap);
        box.appendChild(saveP);
      }
    }
    card.appendChild(box);
  }
  root.appendChild(card);
}
</script>
</body>
</html>
"""


# ============================================================
# HTTP-СЕРВЕР ДЛЯ RENDER (СТАБИЛЬНЫЙ HTTP/1.0 + ПЕРЕХВАТ ОШИБОК)
# ============================================================
class AdminHttpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, format, *args):
        pass

    def _send_bytes(self, code: int, content_type: str, raw: bytes):
        try:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Connection", "close")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_json(self, code: int, obj: dict):
        self._send_bytes(code, "application/json; charset=utf-8", json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def _read_json(self) -> dict:
        cl = int(self.headers.get("Content-Length", "0") or "0")
        if cl <= 0:
            return {}
        if cl > MAX_JSON_BODY_BYTES:
            raise ValueError("Payload too large (max 8 MB)")
        raw = self.rfile.read(cl)
        return json.loads(raw.decode("utf-8"))

    def _get_auth_admin(self) -> dict | None:
        auth_h = self.headers.get("Authorization", "")
        if not auth_h.startswith("Bearer "):
            return None
        token = auth_h[7:].strip()
        if not token:
            return None

        def _op(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.admin_uid, a.username, a.role, a.permissions_json
                    FROM banka_bot_web_sessions s
                    JOIN banka_bot_web_admins a ON a.admin_uid = s.admin_uid
                    WHERE s.token = %s AND s.expires_at > CURRENT_TIMESTAMP
                    LIMIT 1;
                    """,
                    (token,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "admin_uid": int(row[0]),
                    "username": row[1],
                    "role": row[2],
                    "permissions": parse_perms(row[3]),
                    "token": token,
                }

        return with_db(_op)

    def _require_perm(self, perm: str | None = None) -> dict | None:
        admin = self._get_auth_admin()
        if not admin:
            self._send_json(401, {"error": "Требуется авторизация"})
            return None
        if admin["role"] == "USER":
            self._send_json(403, {"error": "Вы не админ и не модератор"})
            return None
        if perm and not user_has_perm(admin, perm):
            self._send_json(403, {"error": f"Нет доступа ({perm})"})
            return None
        return admin

    def do_GET(self):
        try:
            self._dispatch_get()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            print(f"[HTTP GET DB ERROR] {exc}", flush=True)
            self._send_json(503, {"error": "Временная ошибка подключения к БД. Повторите запрос."})
        except Exception as exc:
            print(f"[HTTP GET ERROR] {exc}", flush=True)
            self._send_json(500, {"error": f"Внутренняя ошибка сервера: {exc}"})

    def do_POST(self):
        try:
            self._dispatch_post()
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            print(f"[HTTP POST DB ERROR] {exc}", flush=True)
            self._send_json(503, {"error": "Временная ошибка подключения к БД. Повторите запрос."})
        except Exception as exc:
            print(f"[HTTP POST ERROR] {exc}", flush=True)
            self._send_json(500, {"error": f"Внутренняя ошибка сервера: {exc}"})

    def _dispatch_get(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send_bytes(200, "text/html; charset=utf-8", PUBLIC_HTML.encode("utf-8"))
            return
        if path in ("/admin", "/admin/"):
            self._send_bytes(200, "text/html; charset=utf-8", ADMIN_HTML.encode("utf-8"))
            return
        if path == "/health":
            self._send_json(200, {"ok": True, "db_ready": _db_initialized})
            return

        if path == "/api/auth/me":
            admin = self._get_auth_admin()
            if not admin:
                self._send_json(401, {"error": "Не авторизован"})
                return
            self._send_json(200, admin)
            return

        if path == "/api/dashboard":
            if self._require_perm("view_dashboard"):
                self._handle_dashboard()
            return

        if path == "/api/users":
            if self._require_perm("view_users"):
                q = (qs.get("q", [""])[0] or "").strip()
                self._handle_bot_users(q)
            return

        if path == "/api/gdz-users":
            admin = self._get_auth_admin()
            if not admin or not (user_has_perm(admin, "view_users") or user_has_perm(admin, "view_chats")):
                self._send_json(403, {"error": "Нет доступа"})
                return
            q = (qs.get("q", [""])[0] or "").strip()
            self._handle_gdz_users(q)
            return

        if path.startswith("/api/gdz-users/") and path.endswith("/chats"):
            if self._require_perm("view_chats"):
                try:
                    gdz_uid = int(path.split("/")[3])
                except Exception:
                    self._send_json(400, {"error": "Некорректный ID"})
                    return
                self._handle_gdz_user_chats(gdz_uid)
            return

        if path.startswith("/api/gdz-users/") and path.endswith("/detail"):
            if self._require_perm("view_users"):
                try:
                    gdz_uid = int(path.split("/")[3])
                except Exception:
                    self._send_json(400, {"error": "Некорректный ID"})
                    return
                self._handle_gdz_user_detail(gdz_uid)
            return

        if path.startswith("/api/users/") and path.endswith("/chats"):
            if self._require_perm("view_chats"):
                try:
                    tg_id = int(path.split("/")[3])
                except Exception:
                    self._send_json(400, {"error": "Некорректный ID"})
                    return
                self._handle_tg_user_chats(tg_id)
            return

        if path.startswith("/api/users/"):
            if self._require_perm("view_users"):
                try:
                    tg_id = int(path.split("/")[3])
                except Exception:
                    self._send_json(400, {"error": "Некорректный ID"})
                    return
                self._handle_tg_user_detail(tg_id)
            return

        if path.startswith("/api/chats/messages/") and path.endswith("/media"):
            if self._require_perm("view_chats"):
                try:
                    msg_uid = int(path.split("/")[4])
                except Exception:
                    self._send_json(400, {"error": "Некорректный msg_uid"})
                    return
                self._handle_chat_message_media(msg_uid)
            return

        if path.startswith("/api/chats/") and path.endswith("/messages"):
            if self._require_perm("view_chats"):
                try:
                    room_uid = int(path.split("/")[3])
                except Exception:
                    self._send_json(400, {"error": "Некорректный room_uid"})
                    return
                self._handle_chat_messages(room_uid)
            return

        if path == "/api/moderators":
            admin = self._require_perm(None)
            if not admin:
                return
            if admin["role"] != "SUPER_ADMIN":
                self._send_json(403, {"error": "Только для Супер-администратора"})
                return
            self._handle_list_moderators()
            return

        self._send_json(404, {"error": "Not found"})

    def _dispatch_post(self):
        path = urlparse(self.path).path
        try:
            body = self._read_json()
        except ValueError as e:
            self._send_json(413 if "large" in str(e).lower() else 400, {"error": str(e)})
            return
        except Exception:
            self._send_json(400, {"error": "Некорректный JSON"})
            return

        if path == "/api/auth/register":
            self._handle_register(body)
            return
        if path == "/api/auth/login":
            self._handle_login(body)
            return
        if path == "/api/auth/logout":
            admin = self._get_auth_admin()
            if admin:
                with_db(lambda conn: (
                    conn.cursor().execute("DELETE FROM banka_bot_web_sessions WHERE token = %s;", (admin["token"],)),
                    conn.commit(),
                ))
            self._send_json(200, {"ok": True})
            return

        if path == "/api/messages/dm":
            admin = self._require_perm("send_dm")
            if admin:
                self._handle_dm(admin, body)
            return

        if path == "/api/messages/broadcast":
            admin = self._require_perm("broadcast")
            if admin:
                self._handle_broadcast(admin, body)
            return

        if path == "/api/moderators/role":
            admin = self._require_perm(None)
            if not admin:
                return
            if admin["role"] != "SUPER_ADMIN":
                self._send_json(403, {"error": "Только для Супер-администратора"})
                return
            self._handle_set_mod_role(admin, body)
            return

        if path == "/api/moderators/perms":
            admin = self._require_perm(None)
            if not admin:
                return
            if admin["role"] != "SUPER_ADMIN":
                self._send_json(403, {"error": "Только для Супер-администратора"})
                return
            self._handle_set_mod_perms(body)
            return

        self._send_json(404, {"error": "Not found"})

    # -------- Handlers --------
    def _handle_register(self, body: dict):
        username = str(body.get("username", "")).strip()
        password = str(body.get("password", ""))
        if len(username) < 3 or len(username) > 50 or any(c.isspace() for c in username) or len(password) < 4:
            self._send_json(400, {"error": "Логин от 3 до 50 символов без пробелов, пароль от 4 символов"})
            return
        pw_hash = hash_password(password)
        all_perm_json = json.dumps([k for k, _ in ALL_PERMISSIONS])

        def _op(conn):
            with conn.cursor() as cur:
                cur.execute("LOCK TABLE banka_bot_web_admins IN EXCLUSIVE MODE;")
                cur.execute("SELECT COUNT(*) FROM banka_bot_web_admins WHERE role = 'SUPER_ADMIN';")
                super_cnt = int(cur.fetchone()[0])
                role = "SUPER_ADMIN" if super_cnt == 0 else "USER"
                perms_json = all_perm_json if super_cnt == 0 else "[]"
                cur.execute("SELECT 1 FROM banka_bot_web_admins WHERE LOWER(username) = LOWER(%s) LIMIT 1;", (username,))
                if cur.fetchone():
                    return None, "exists"
                cur.execute(
                    """
                    INSERT INTO banka_bot_web_admins (username, password_hash, role, permissions_json)
                    VALUES (%s, %s, %s, %s)
                    RETURNING admin_uid;
                    """,
                    (username, pw_hash, role, perms_json),
                )
                uid = int(cur.fetchone()[0])
            conn.commit()
            return (uid, role), None

        res, err = with_db(_op)
        if err == "exists":
            self._send_json(409, {"error": "Такой логин уже занят"})
            return
        uid, role = res
        self._send_json(200, {"admin_uid": uid, "username": username, "role": role})

    def _handle_login(self, body: dict):
        username = str(body.get("username", "")).strip()
        password = str(body.get("password", ""))

        def _op(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT admin_uid, username, password_hash, role, permissions_json FROM banka_bot_web_admins WHERE LOWER(username) = LOWER(%s) LIMIT 1;",
                    (username,),
                )
                row = cur.fetchone()
                if not row or not verify_password(password, row[2]):
                    return None
                uid, uname, _, role, p_json = row
                if role == "USER":
                    return {"role": "USER", "username": uname}
                token = secrets.token_urlsafe(48)
                cur.execute(
                    "INSERT INTO banka_bot_web_sessions (token, admin_uid, expires_at) VALUES (%s, %s, CURRENT_TIMESTAMP + INTERVAL '7 days');",
                    (token, int(uid)),
                )
                cur.execute("UPDATE banka_bot_web_admins SET last_login_at = CURRENT_TIMESTAMP WHERE admin_uid = %s;", (int(uid),))
            conn.commit()
            return {
                "token": token,
                "admin_uid": int(uid),
                "username": uname,
                "role": role,
                "permissions": parse_perms(p_json),
            }

        res = with_db(_op)
        if not res:
            self._send_json(401, {"error": "Неверный логин или пароль"})
            return
        self._send_json(200, res)

    def _handle_dashboard(self):
        msk_now = datetime.now(MSK_TZ)
        today_start_ms = int(datetime(msk_now.year, msk_now.month, msk_now.day, tzinfo=MSK_TZ).timestamp() * 1000)
        week_start_ms = now_ms() - 7 * 86_400_000
        month_start_ms = now_ms() - 30 * 86_400_000

        def _op(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN stars_amount ELSE 0 END), 0),
                        COUNT(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN 1 END),
                        COALESCE(SUM(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN stars_amount ELSE 0 END), 0),
                        COUNT(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN 1 END),
                        COALESCE(SUM(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN stars_amount ELSE 0 END), 0),
                        COUNT(CASE WHEN status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s THEN 1 END),
                        COALESCE(SUM(CASE WHEN status_tag = 'PAID' THEN stars_amount ELSE 0 END), 0),
                        COUNT(CASE WHEN status_tag = 'PAID' THEN 1 END),
                        COUNT(CASE WHEN status_tag = 'PENDING' THEN 1 END),
                        COUNT(CASE WHEN status_tag IN ('CANCELLED', 'REFUNDED') THEN 1 END)
                    FROM banka_stars_orders;
                    """,
                    (today_start_ms, today_start_ms, week_start_ms, week_start_ms, month_start_ms, month_start_ms),
                )
                s = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM banka_bot_users;")
                bot_cnt = int(cur.fetchone()[0])

                gdz_cnt = 0
                if _table_exists(cur, "banka_profiles_vault"):
                    cur.execute("SELECT COUNT(*) FROM banka_profiles_vault;")
                    gdz_cnt = int(cur.fetchone()[0])

                rooms_cnt, msgs_cnt = 0, 0
                if _table_exists(cur, "banka_chat_rooms") and _table_exists(cur, "banka_chat_messages"):
                    cur.execute("SELECT COUNT(*) FROM banka_chat_rooms;")
                    rooms_cnt = int(cur.fetchone()[0])
                    cur.execute("SELECT COUNT(*) FROM banka_chat_messages;")
                    msgs_cnt = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT order_uid, buyer_uid, buyer_login, item_kind, plan_days,
                           coins_amount, target_bg_id, stars_amount, status_tag,
                           telegram_user_id, COALESCE(paid_ms, created_ms)
                    FROM banka_stars_orders
                    ORDER BY order_uid DESC LIMIT 20;
                    """
                )
                recent = [
                    {
                        "order_uid": int(r[0]),
                        "buyer_uid": int(r[1]),
                        "buyer_login": r[2] or "",
                        "item_label": _format_item_label(r[3], r[4], r[5], r[6]),
                        "stars_amount": int(r[7]),
                        "status_tag": r[8],
                        "telegram_user_id": int(r[9] or 0),
                        "created_msk": fmt_msk(r[10]),
                    }
                    for r in cur.fetchall()
                ]
            return {
                "stars_today": int(s[0]),
                "orders_today": int(s[1]),
                "stars_week": int(s[2]),
                "orders_week": int(s[3]),
                "stars_month": int(s[4]),
                "orders_month": int(s[5]),
                "stars_all": int(s[6]),
                "orders_all": int(s[7]),
                "pending_orders": int(s[8]),
                "cancelled_orders": int(s[9]),
                "bot_users_count": bot_cnt,
                "gdz_users_count": gdz_cnt,
                "chat_rooms_count": rooms_cnt,
                "chat_messages_count": msgs_cnt,
                "recent_orders": recent,
            }

        self._send_json(200, with_db(_op))

    def _handle_bot_users(self, q: str):
        like = f"%{q}%"

        def _op(conn):
            with conn.cursor() as cur:
                has_vault = _table_exists(cur, "banka_profiles_vault")
                join_vault = "LEFT JOIN banka_profiles_vault v ON v.rec_uid = b.gdz_user_id" if has_vault else ""
                vault_cols = "COALESCE(NULLIF(TRIM(v.title_first || ' ' || v.title_last), ''), '') AS gdz_fullname" if has_vault else "'' AS gdz_fullname"
                where_sql = ""
                params = []
                if q:
                    where_sql = """
                        WHERE CAST(b.tg_user_id AS TEXT) ILIKE %s
                           OR b.tg_username ILIKE %s
                           OR b.tg_first_name ILIKE %s
                           OR b.tg_last_name ILIKE %s
                           OR b.gdz_login ILIKE %s
                    """
                    params = [like, like, like, like, like]
                cur.execute(
                    f"""
                    SELECT b.tg_user_id, b.tg_username, b.tg_first_name, b.tg_last_name,
                           b.gdz_user_id, b.gdz_login, {vault_cols},
                           b.last_seen_at,
                           COALESCE((SELECT SUM(o.stars_amount) FROM banka_stars_orders o WHERE (o.telegram_user_id = b.tg_user_id OR (b.gdz_user_id > 0 AND o.buyer_uid = b.gdz_user_id)) AND o.status_tag = 'PAID'), 0) AS spent_stars,
                           COALESCE((SELECT COUNT(*) FROM banka_stars_orders o WHERE (o.telegram_user_id = b.tg_user_id OR (b.gdz_user_id > 0 AND o.buyer_uid = b.gdz_user_id)) AND o.status_tag = 'PAID'), 0) AS paid_cnt
                    FROM banka_bot_users b
                    {join_vault}
                    {where_sql}
                    ORDER BY b.last_seen_at DESC
                    LIMIT 200;
                    """,
                    params,
                )
                return [
                    {
                        "tg_user_id": int(r[0]),
                        "tg_username": r[1] or "",
                        "tg_first_name": r[2] or "",
                        "tg_last_name": r[3] or "",
                        "gdz_user_id": int(r[4] or 0),
                        "gdz_login": r[5] or "",
                        "gdz_fullname": r[6] or "",
                        "last_seen_msk": fmt_msk(r[7]),
                        "spent_stars": int(r[8]),
                        "paid_orders_count": int(r[9]),
                    }
                    for r in cur.fetchall()
                ]

        self._send_json(200, {"users": with_db(_op)})

    def _handle_gdz_users(self, q: str):
        like = f"%{q}%"

        def _op(conn):
            with conn.cursor() as cur:
                if not _table_exists(cur, "banka_profiles_vault"):
                    return []
                has_chats = _table_exists(cur, "banka_chat_members")
                chats_expr = "(SELECT COUNT(*) FROM banka_chat_members cm WHERE cm.member_uid = v.rec_uid)" if has_chats else "0"
                where_sql = ""
                params = [now_ms()]
                if q:
                    where_sql = """
                        WHERE CAST(v.rec_uid AS TEXT) ILIKE %s
                           OR v.entry_tag ILIKE %s
                           OR v.title_first ILIKE %s
                           OR v.title_last ILIKE %s
                           OR v.cohort_code ILIKE %s
                    """
                    params.extend([like, like, like, like, like])
                cur.execute(
                    f"""
                    SELECT v.rec_uid, v.entry_tag,
                           TRIM(COALESCE(v.title_first, '') || ' ' || COALESCE(v.title_last, '')),
                           COALESCE(v.cohort_code, ''),
                           COALESCE(v.rank_badge, 'STUDENT'),
                           (COALESCE(v.vip_flag, FALSE) AND (v.premium_until_ms IS NULL OR v.premium_until_ms > %s)),
                           COALESCE(v.vault_tokens, 0),
                           {chats_expr} AS chats_cnt,
                           COALESCE((SELECT SUM(o.stars_amount) FROM banka_stars_orders o WHERE o.buyer_uid = v.rec_uid AND o.status_tag = 'PAID'), 0) AS spent_stars,
                           COALESCE((SELECT MAX(b.tg_user_id) FROM banka_bot_users b WHERE b.gdz_user_id = v.rec_uid), 0) AS tg_uid
                    FROM banka_profiles_vault v
                    {where_sql}
                    ORDER BY v.rec_uid DESC
                    LIMIT 250;
                    """,
                    params,
                )
                return [
                    {
                        "rec_uid": int(r[0]),
                        "login": r[1] or "",
                        "full_name": (r[2] or "").strip(),
                        "cohort_code": r[3] or "",
                        "rank_badge": r[4] or "STUDENT",
                        "is_vip": bool(r[5]),
                        "coins": int(r[6]),
                        "chats_count": int(r[7]),
                        "spent_stars": int(r[8]),
                        "tg_user_id": int(r[9] or 0),
                    }
                    for r in cur.fetchall()
                ]

        self._send_json(200, {"users": with_db(_op)})

    def _fetch_orders_and_dms(self, cur, tg_id: int, gdz_uid: int):
        cur.execute(
            """
            SELECT order_uid, pay_code, item_kind, plan_days, coins_amount, target_bg_id,
                   stars_amount, status_tag, COALESCE(paid_ms, created_ms)
            FROM banka_stars_orders
            WHERE (%s > 0 AND telegram_user_id = %s) OR (%s > 0 AND buyer_uid = %s)
            ORDER BY order_uid DESC
            LIMIT 100;
            """,
            (tg_id, tg_id, gdz_uid, gdz_uid),
        )
        orders = [
            {
                "order_uid": int(r[0]),
                "pay_code": r[1],
                "item_label": _format_item_label(r[2], r[3], r[4], r[5]),
                "stars_amount": int(r[6]),
                "status_tag": r[7],
                "created_msk": fmt_msk(r[8]),
            }
            for r in cur.fetchall()
        ]
        msgs = []
        if tg_id > 0:
            cur.execute(
                """
                SELECT sender_admin, message_text, COALESCE(has_photo, FALSE), created_at
                FROM banka_bot_messages_log
                WHERE target_tg_id = %s AND target_type = 'DM'
                ORDER BY log_uid DESC LIMIT 40;
                """,
                (tg_id,),
            )
            msgs = [
                {
                    "sender_admin": r[0],
                    "message_text": r[1],
                    "has_photo": bool(r[2]),
                    "created_msk": fmt_msk(r[3]),
                }
                for r in cur.fetchall()
            ]
        return orders, msgs

    def _handle_tg_user_detail(self, tg_id: int):
        def _op(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT gdz_user_id FROM banka_bot_users WHERE tg_user_id = %s LIMIT 1;", (tg_id,))
                row = cur.fetchone()
                gdz_uid = int(row[0]) if (row and row[0]) else 0
                orders, msgs = self._fetch_orders_and_dms(cur, tg_id, gdz_uid)
                return {"tg_user_id": tg_id, "gdz_user_id": gdz_uid, "orders": orders, "messages": msgs}

        self._send_json(200, with_db(_op))

    def _handle_gdz_user_detail(self, gdz_uid: int):
        def _op(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(tg_user_id), 0) FROM banka_bot_users WHERE gdz_user_id = %s;", (gdz_uid,))
                tg_id = int(cur.fetchone()[0] or 0)
                orders, msgs = self._fetch_orders_and_dms(cur, tg_id, gdz_uid)
                return {"tg_user_id": tg_id, "gdz_user_id": gdz_uid, "orders": orders, "messages": msgs}

        self._send_json(200, with_db(_op))

    def _handle_tg_user_chats(self, tg_id: int):
        def _find_gdz(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT gdz_user_id FROM banka_bot_users WHERE tg_user_id = %s LIMIT 1;", (tg_id,))
                row = cur.fetchone()
                return int(row[0]) if (row and row[0]) else 0

        gdz_uid = with_db(_find_gdz)
        if gdz_uid <= 0:
            self._send_json(200, {"rooms": []})
            return
        self._handle_gdz_user_chats(gdz_uid)

    def _handle_gdz_user_chats(self, gdz_uid: int):
        def _op(conn):
            with conn.cursor() as cur:
                if not (_table_exists(cur, "banka_chat_rooms") and _table_exists(cur, "banka_chat_members") and _table_exists(cur, "banka_chat_messages")):
                    return []
                cur.execute(
                    """
                    SELECT
                        r.room_uid,
                        r.kind_tag,
                        COALESCE(r.title_txt, ''),
                        (SELECT COUNT(*) FROM banka_chat_messages m WHERE m.room_uid = r.room_uid) AS msg_cnt,
                        COALESCE((SELECT MAX(m2.created_stamp) FROM banka_chat_messages m2 WHERE m2.room_uid = r.room_uid), r.updated_stamp) AS last_ts,
                        COALESCE(NULLIF(TRIM(peer.title_first || ' ' || peer.title_last), ''), peer.entry_tag, '') AS peer_name,
                        COALESCE(peer.entry_tag, '') AS peer_login
                    FROM banka_chat_rooms r
                    JOIN banka_chat_members my_m ON my_m.room_uid = r.room_uid AND my_m.member_uid = %s
                    LEFT JOIN banka_chat_members pm ON r.kind_tag = 'DIRECT' AND pm.room_uid = r.room_uid AND pm.member_uid <> %s
                    LEFT JOIN banka_profiles_vault peer ON peer.rec_uid = pm.member_uid
                    ORDER BY last_ts DESC, r.room_uid DESC
                    LIMIT 150;
                    """,
                    (gdz_uid, gdz_uid),
                )
                rooms = []
                for r in cur.fetchall():
                    kind = r[1] or "DIRECT"
                    title = (r[2] or "").strip()
                    peer_name = (r[5] or "").strip()
                    peer_login = (r[6] or "").strip()
                    if kind == "DIRECT":
                        disp = f"ЛС с {peer_name}" + (f" (@{peer_login})" if peer_login else "") if peer_name else f"Личный чат #{r[0]}"
                    else:
                        disp = title or f"Группа #{r[0]}"
                    rooms.append(
                        {
                            "room_uid": int(r[0]),
                            "kind_tag": kind,
                            "display_title": disp,
                            "messages_count": int(r[3]),
                            "updated_msk": fmt_msk(r[4]),
                        }
                    )
                return rooms

        self._send_json(200, {"rooms": with_db(_op)})

    def _handle_chat_messages(self, room_uid: int):
        def _op(conn):
            with conn.cursor() as cur:
                if not _table_exists(cur, "banka_chat_messages"):
                    return []
                cur.execute(
                    """
                    SELECT
                        m.msg_uid,
                        m.sender_uid,
                        COALESCE(NULLIF(TRIM(u.title_first || ' ' || u.title_last), ''), u.entry_tag, 'Пользователь') AS sender_name,
                        COALESCE(u.entry_tag, '') AS sender_login,
                        m.kind_tag,
                        COALESCE(m.text_body, ''),
                        COALESCE(m.reply_sender_name, ''),
                        COALESCE(m.reply_preview, ''),
                        COALESCE(m.forwarded_from_name, ''),
                        COALESCE(m.media_name, ''),
                        (COALESCE(m.media_b64, '') <> '') AS has_media,
                        COALESCE(m.voice_sec, 0),
                        COALESCE(m.shared_mat_title, ''),
                        COALESCE(m.shared_ai_title, ''),
                        COALESCE(m.is_edited, FALSE),
                        COALESCE(m.is_deleted, FALSE),
                        m.created_stamp
                    FROM banka_chat_messages m
                    LEFT JOIN banka_profiles_vault u ON u.rec_uid = m.sender_uid
                    WHERE m.room_uid = %s
                    ORDER BY m.msg_uid DESC
                    LIMIT 250;
                    """,
                    (room_uid,),
                )
                rows = list(reversed(cur.fetchall()))
                return [
                    {
                        "msg_uid": int(r[0]),
                        "sender_uid": int(r[1]),
                        "sender_name": r[2],
                        "sender_login": r[3],
                        "kind_tag": r[4],
                        "text_body": r[5],
                        "reply_sender_name": r[6],
                        "reply_preview": r[7],
                        "forwarded_from_name": r[8],
                        "media_name": r[9],
                        "has_media": bool(r[10]),
                        "voice_sec": int(r[11]),
                        "shared_mat_title": r[12],
                        "shared_ai_title": r[13],
                        "is_edited": bool(r[14]),
                        "is_deleted": bool(r[15]),
                        "created_msk": fmt_msk(r[16]),
                    }
                    for r in rows
                ]

        self._send_json(200, {"messages": with_db(_op)})

    def _handle_chat_message_media(self, msg_uid: int):
        def _op(conn):
            with conn.cursor() as cur:
                if not _table_exists(cur, "banka_chat_messages"):
                    return None
                cur.execute("SELECT COALESCE(media_b64, '') FROM banka_chat_messages WHERE msg_uid = %s LIMIT 1;", (msg_uid,))
                row = cur.fetchone()
                return row[0] if (row and row[0]) else None

        b64 = with_db(_op)
        if not b64:
            self._send_json(404, {"error": "Фото отсутствует"})
            return
        self._send_json(200, {"media_b64": b64})

    def _handle_dm(self, admin: dict, body: dict):
        try:
            tg_id = int(body.get("tg_user_id", 0))
        except Exception:
            tg_id = 0
        text = str(body.get("text", "")).strip()
        raw_photo = str(body.get("photo_b64", "")).strip()
        if tg_id <= 0 or (not text and not raw_photo):
            self._send_json(400, {"error": "Укажите получателя и текст или фото"})
            return
        try:
            photo_bytes, photo_name = parse_image_data_url(raw_photo)
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
            return

        try:
            send_telegram_content(tg_id, text, photo_bytes, photo_name)
        except Exception as e:
            self._send_json(400, {"error": f"Telegram отклонил отправку: {e}"})
            return

        with_db(lambda conn: (
            conn.cursor().execute(
                """
                INSERT INTO banka_bot_messages_log (sender_admin, target_type, target_tg_id, message_text, has_photo, sent_count, failed_count)
                VALUES (%s, 'DM', %s, %s, %s, 1, 0);
                """,
                (admin["username"], tg_id, text, bool(photo_bytes)),
            ),
            conn.commit(),
        ))
        self._send_json(200, {"ok": True})

    def _handle_broadcast(self, admin: dict, body: dict):
        audience = str(body.get("audience", "all")).strip()
        text = str(body.get("text", "")).strip()
        raw_photo = str(body.get("photo_b64", "")).strip()
        if not text and not raw_photo:
            self._send_json(400, {"error": "Добавьте текст или фото для рассылки"})
            return
        try:
            photo_bytes, photo_name = parse_image_data_url(raw_photo)
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
            return

        def _get_targets(conn):
            with conn.cursor() as cur:
                if audience == "linked":
                    cur.execute("SELECT tg_user_id FROM banka_bot_users WHERE COALESCE(is_blocked, FALSE) = FALSE AND gdz_user_id > 0;")
                elif audience == "buyers":
                    cur.execute(
                        """
                        SELECT DISTINCT b.tg_user_id
                        FROM banka_bot_users b
                        JOIN banka_stars_orders o ON o.telegram_user_id = b.tg_user_id
                        WHERE COALESCE(b.is_blocked, FALSE) = FALSE AND o.status_tag = 'PAID';
                        """
                    )
                else:
                    cur.execute("SELECT tg_user_id FROM banka_bot_users WHERE COALESCE(is_blocked, FALSE) = FALSE;")
                return [int(r[0]) for r in cur.fetchall()]

        targets = with_db(_get_targets)
        sent_ok = 0
        failed = 0
        cached_file_id = None
        blocked_ids = []

        for tid in targets:
            try:
                cached_file_id = send_telegram_content(tid, text, photo_bytes, photo_name, cached_file_id=cached_file_id)
                sent_ok += 1
            except Exception as exc:
                failed += 1
                msg_low = str(exc).lower()
                if "blocked" in msg_low or "deactivated" in msg_low or "chat not found" in msg_low:
                    blocked_ids.append(tid)
            time.sleep(0.04)

        def _log_bc(conn):
            with conn.cursor() as cur:
                if blocked_ids:
                    cur.execute(
                        "UPDATE banka_bot_users SET is_blocked = TRUE WHERE tg_user_id = ANY(%s);",
                        (blocked_ids,),
                    )
                cur.execute(
                    """
                    INSERT INTO banka_bot_messages_log (sender_admin, target_type, target_tg_id, message_text, has_photo, sent_count, failed_count)
                    VALUES (%s, %s, 0, %s, %s, %s, %s);
                    """,
                    (admin["username"], f"BROADCAST:{audience}", text, bool(photo_bytes), sent_ok, failed),
                )
            conn.commit()

        with_db(_log_bc)
        self._send_json(200, {"ok": True, "sent": sent_ok, "failed": failed, "total": len(targets), "has_photo": bool(photo_bytes)})

    def _handle_list_moderators(self):
        def _op(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT admin_uid, username, role, permissions_json, created_at FROM banka_bot_web_admins ORDER BY admin_uid ASC;")
                return [
                    {
                        "admin_uid": int(r[0]),
                        "username": r[1],
                        "role": r[2],
                        "permissions": parse_perms(r[3]),
                        "created_msk": fmt_msk(r[4]),
                    }
                    for r in cur.fetchall()
                ]

        self._send_json(
            200,
            {
                "admins": with_db(_op),
                "all_permissions": [{"key": k, "label": lbl} for k, lbl in ALL_PERMISSIONS],
            },
        )

    def _handle_set_mod_role(self, actor: dict, body: dict):
        uid = int(body.get("admin_uid", 0))
        role = str(body.get("role", "USER")).upper()
        if role not in ("USER", "MODERATOR"):
            self._send_json(400, {"error": "Допустимы только роли USER и MODERATOR"})
            return
        if uid == actor["admin_uid"]:
            self._send_json(400, {"error": "Нельзя изменить собственную роль"})
            return

        def _op(conn):
            with conn.cursor() as cur:
                cur.execute("UPDATE banka_bot_web_admins SET role = %s WHERE admin_uid = %s AND role <> 'SUPER_ADMIN';", (role, uid))
                if role == "USER":
                    cur.execute("DELETE FROM banka_bot_web_sessions WHERE admin_uid = %s;", (uid,))
            conn.commit()

        with_db(_op)
        self._send_json(200, {"ok": True})

    def _handle_set_mod_perms(self, body: dict):
        uid = int(body.get("admin_uid", 0))
        perms = [str(p) for p in (body.get("permissions") or []) if str(p) in PERM_KEYS]
        with_db(lambda conn: (
            conn.cursor().execute(
                "UPDATE banka_bot_web_admins SET permissions_json = %s WHERE admin_uid = %s AND role = 'MODERATOR';",
                (json.dumps(perms), uid),
            ),
            conn.commit(),
        ))
        self._send_json(200, {"ok": True})


def _format_item_label(kind: str, days: int, coins: int, bg_id: int) -> str:
    k = str(kind or "").upper()
    if k == "PREMIUM":
        return f"Premium ({days} дн.)"
    if k == "COINS":
        return f"Монеты (+{coins})"
    if k == "BACKGROUND":
        return f"Фон профиля #{bg_id}"
    return k or "Заказ"


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def run_bot_polling():
    while True:
        try:
            ensure_db_ready()
            bot.infinity_polling(timeout=30, long_polling_timeout=25, skip_pending=False)
        except Exception as e:
            print(f"[BOT POLLING ERROR] {e}", flush=True)
            time.sleep(3)


def main():
    # Сначала открываем порт для Render, чтобы не было 502 на старте
    server = ReusableThreadingHTTPServer(("0.0.0.0", PORT), AdminHttpHandler)
    print(f"GDZgo Bot & Admin server listening on 0.0.0.0:{PORT}", flush=True)

    threading.Thread(target=init_db_background, daemon=True).start()
    threading.Thread(target=cancelled_invoice_watcher, daemon=True).start()
    threading.Thread(target=run_bot_polling, daemon=True).start()

    server.serve_forever()


if __name__ == "__main__":
    main()
