from __future__ import annotations

import hashlib
import hmac
import html
import json
import math
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row
import telebot
from telebot import types

# -----------------------------
# Конфигурация из Environment
# -----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN")

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("Не задана переменная окружения DATABASE_URL")

PORT = int(os.environ.get("PORT", "10000"))
MSK_TZ = timezone(timedelta(hours=3))

ROLE_SUPER_ADMIN = "SUPER_ADMIN"
ROLE_MODERATOR = "MODERATOR"
ROLE_USER = "USER"

ALL_PERMISSIONS: dict[str, str] = {
    "view_stats": "Просмотр доходов и статистики",
    "view_users": "Просмотр пользователей бота и истории ⭐",
    "view_orders": "Просмотр списка всех заказов Stars",
    "send_dm": "Отправка личных сообщений в бота",
    "send_broadcast": "Массовая рассылка всем пользователям бота",
    "manage_orders": "Отмена активных (PENDING) заказов",
}

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
user_states: dict[int, dict[str, Any]] = {}
state_lock = threading.Lock()


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_msk(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "—"
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=MSK_TZ)
    return dt.strftime("%d.%m.%Y %H:%M:%S МСК")


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=12,
        row_factory=dict_row,
    )


def hash_password(password: str, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else os.urandom(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=16384,
        r=8,
        p=1,
        maxmem=32 * 1024 * 1024,
    )
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, expected_hash_hex: str) -> bool:
    _, calc = hash_password(password, salt_hex)
    return hmac.compare_digest(calc, expected_hash_hex)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def safe_json_dict(raw: Any) -> dict[str, bool]:
    if isinstance(raw, dict):
        return {k: bool(v) for k, v in raw.items() if k in ALL_PERMISSIONS}
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return {k: bool(v) for k, v in parsed.items() if k in ALL_PERMISSIONS}
        except Exception:
            pass
    return {}


def init_bot_tables() -> None:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS banka_bot_users (
                    tg_user_id BIGINT PRIMARY KEY,
                    tg_username VARCHAR(120) DEFAULT '',
                    first_name VARCHAR(160) DEFAULT '',
                    last_name VARCHAR(160) DEFAULT '',
                    linked_gdz_uid INT DEFAULT 0,
                    linked_gdz_login VARCHAR(120) DEFAULT '',
                    first_seen_ms BIGINT NOT NULL,
                    last_seen_ms BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bot_users_seen
                    ON banka_bot_users (last_seen_ms DESC);

                CREATE TABLE IF NOT EXISTS banka_stars_invoices (
                    invoice_uid SERIAL PRIMARY KEY,
                    order_uid INT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    cleaned_flag BOOLEAN DEFAULT FALSE,
                    created_ms BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_stars_inv_clean
                    ON banka_stars_invoices (cleaned_flag, order_uid);

                CREATE TABLE IF NOT EXISTS banka_bot_web_admins (
                    web_uid SERIAL PRIMARY KEY,
                    login VARCHAR(80) UNIQUE NOT NULL,
                    display_name VARCHAR(120) DEFAULT '',
                    pwd_salt VARCHAR(64) NOT NULL,
                    pwd_hash VARCHAR(256) NOT NULL,
                    role VARCHAR(24) NOT NULL DEFAULT 'USER',
                    perms_json TEXT NOT NULL DEFAULT '{}',
                    created_ms BIGINT NOT NULL,
                    last_login_ms BIGINT DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS banka_bot_web_sessions (
                    token_hash VARCHAR(128) PRIMARY KEY,
                    web_uid INT NOT NULL REFERENCES banka_bot_web_admins(web_uid) ON DELETE CASCADE,
                    created_ms BIGINT NOT NULL,
                    expires_ms BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_web_sess_exp
                    ON banka_bot_web_sessions (expires_ms);

                CREATE TABLE IF NOT EXISTS banka_bot_messages_log (
                    log_uid SERIAL PRIMARY KEY,
                    sender_web_uid INT NOT NULL,
                    sender_login VARCHAR(80) NOT NULL,
                    kind VARCHAR(24) NOT NULL,
                    target_tg_user_id BIGINT DEFAULT 0,
                    message_text TEXT NOT NULL,
                    sent_count INT DEFAULT 0,
                    failed_count INT DEFAULT 0,
                    created_ms BIGINT NOT NULL
                );
                """
            )


def upsert_bot_user(
    tg_user: types.User | None,
    linked_uid: int | None = None,
    linked_login: str | None = None,
) -> None:
    if tg_user is None:
        return
    ts = now_ms()
    uname = (tg_user.username or "").strip().lstrip("@")[:120]
    fname = (tg_user.first_name or "").strip()[:160]
    lname = (tg_user.last_name or "").strip()[:160]
    l_uid = int(linked_uid) if linked_uid and linked_uid > 0 else 0
    l_login = (linked_login or "").strip()[:120]
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO banka_bot_users (
                        tg_user_id, tg_username, first_name, last_name,
                        linked_gdz_uid, linked_gdz_login, first_seen_ms, last_seen_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tg_user_id) DO UPDATE SET
                        tg_username = CASE WHEN EXCLUDED.tg_username <> '' THEN EXCLUDED.tg_username ELSE banka_bot_users.tg_username END,
                        first_name = CASE WHEN EXCLUDED.first_name <> '' THEN EXCLUDED.first_name ELSE banka_bot_users.first_name END,
                        last_name = CASE WHEN EXCLUDED.last_name <> '' THEN EXCLUDED.last_name ELSE banka_bot_users.last_name END,
                        linked_gdz_uid = CASE WHEN EXCLUDED.linked_gdz_uid > 0 THEN EXCLUDED.linked_gdz_uid ELSE banka_bot_users.linked_gdz_uid END,
                        linked_gdz_login = CASE WHEN EXCLUDED.linked_gdz_login <> '' THEN EXCLUDED.linked_gdz_login ELSE banka_bot_users.linked_gdz_login END,
                        last_seen_ms = EXCLUDED.last_seen_ms;
                    """,
                    (tg_user.id, uname, fname, lname, l_uid, l_login, ts, ts),
                )
    except Exception:
        pass


def get_runtime_pricing() -> dict[str, Any]:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(stars_10d_price, 50) AS stars_10d_price,
                       COALESCE(stars_30d_price, 100) AS stars_30d_price,
                       COALESCE(coins_per_star, 5.0) AS coins_per_star,
                       COALESCE(prem_discount_pct, 0) AS prem_discount_pct
                FROM banka_system_config
                WHERE cfg_uid = 1;
                """
            )
            row = cur.fetchone() or {}
    raw10 = max(1, int(row.get("stars_10d_price") or 50))
    raw30 = max(1, int(row.get("stars_30d_price") or 100))
    disc = max(0, min(99, int(row.get("prem_discount_pct") or 0)))
    rate = float(row.get("coins_per_star") or 5.0)
    if rate <= 0:
        rate = 5.0
    eff10 = max(1, int(math.ceil(raw10 * (100 - disc) / 100.0)))
    eff30 = max(1, int(math.ceil(raw30 * (100 - disc) / 100.0)))
    return {
        "stars_10d": eff10,
        "stars_30d": eff30,
        "coins_per_star": rate,
        "discount_pct": disc,
    }


def generate_pay_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))


def describe_order(row: dict[str, Any]) -> tuple[str, str]:
    kind = str(row.get("item_kind") or "").upper()
    if kind == "COINS":
        coins = int(row.get("coins_amount") or 0)
        return (
            f"{coins} монет GDZgo",
            f"Пополнение баланса на {coins} монет для аккаунта @{row.get('buyer_login') or 'user'}",
        )
    if kind == "BACKGROUND":
        bg_title = str(row.get("bg_title") or f"Фон #{row.get('target_bg_id') or 0}")
        return (
            f"Фон «{bg_title}»",
            f"Эксклюзивный фон профиля для аккаунта @{row.get('buyer_login') or 'user'}",
        )
    days = int(row.get("plan_days") or 30)
    return (
        f"GDZgo Premium ({days} дн.)",
        f"Премиум-доступ на {days} дней для аккаунта @{row.get('buyer_login') or 'user'}",
    )


def insert_user_notice(
    cur: psycopg.Cursor,
    target_uid: int,
    kind_tag: str,
    title_txt: str,
    body_txt: str,
    issuer_label: str = "Telegram Stars",
) -> None:
    cur.execute(
        """
        INSERT INTO banka_user_notices (
            target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label, seen_flag
        )
        VALUES (%s, %s, %s, %s, '', %s, FALSE);
        """,
        (target_uid, kind_tag, title_txt[:160], body_txt, issuer_label[:150]),
    )


def grant_timed_premium_in_tx(
    cur: psycopg.Cursor,
    user_id: int,
    days: int,
    stars_amount: int,
) -> None:
    cur.execute(
        "SELECT vip_flag, premium_until_ms FROM banka_profiles_vault WHERE rec_uid = %s FOR UPDATE;",
        (user_id,),
    )
    u = cur.fetchone()
    if not u:
        raise RuntimeError("Пользователь не найден")

    curr_vip = bool(u.get("vip_flag"))
    curr_until = u.get("premium_until_ms")
    cur_ms = now_ms()
    is_permanent_admin_vip = curr_vip and curr_until is None
    if is_permanent_admin_vip:
        new_until = None
    else:
        base = curr_until if (curr_vip and curr_until is not None and int(curr_until) > cur_ms) else cur_ms
        new_until = int(base) + int(days) * 86_400_000

    cur.execute(
        "UPDATE banka_profiles_vault SET vip_flag = TRUE, premium_until_ms = %s WHERE rec_uid = %s;",
        (new_until, user_id),
    )
    insert_user_notice(
        cur,
        user_id,
        "PREMIUM_GRANTED",
        f"Оплата ⭐ {stars_amount} подтверждена!",
        f"Через Telegram Stars оплачен и активирован Premium на {days} дней.",
        "Telegram Stars",
    )

    # Реферальный бонус пригласившему (+30 монет) — как в Android-приложении
    cur.execute(
        """
        SELECT referred_by_uid, entry_tag, title_first, title_last
        FROM banka_profiles_vault
        WHERE rec_uid = %s;
        """,
        (user_id,),
    )
    ref_row = cur.fetchone()
    inviter_id = int(ref_row.get("referred_by_uid") or 0) if ref_row else 0
    if inviter_id > 0 and inviter_id != user_id:
        bonus = 30
        buyer_label = (
            f"{ref_row.get('title_first') or ''} {ref_row.get('title_last') or ''}".strip()
            or (ref_row.get("entry_tag") or "Пользователь")
        )
        cur.execute(
            "UPDATE banka_profiles_vault SET vault_tokens = vault_tokens + %s WHERE rec_uid = %s;",
            (bonus, inviter_id),
        )
        cur.execute(
            "UPDATE banka_profiles_vault SET referral_earned_coins = COALESCE(referral_earned_coins, 0) + %s WHERE rec_uid = %s;",
            (bonus, user_id),
        )
        cur.execute(
            """
            INSERT INTO banka_referral_ledger (inviter_uid, referral_uid, reward_type, coins_delta, note_txt)
            VALUES (%s, %s, 'PREMIUM_BUY', %s, %s);
            """,
            (inviter_id, user_id, bonus, f"Покупка Premium: {days} дней за Telegram Stars"),
        )
        insert_user_notice(
            cur,
            inviter_id,
            "REFERRAL_REWARD",
            f"+{bonus} монет за покупку Premium рефералом",
            f"Ваш реферал {buyer_label} оформил Premium ({days} дней, Telegram Stars). Вам начислено +{bonus} монет!",
            "Реферальная программа",
        )


def delete_uncleaned_invoices_for_order(order_uid: int) -> None:
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT invoice_uid, chat_id, message_id
                    FROM banka_stars_invoices
                    WHERE order_uid = %s AND cleaned_flag = FALSE;
                    """,
                    (order_uid,),
                )
                rows = cur.fetchall()
                for inv in rows:
                    try:
                        bot.delete_message(int(inv["chat_id"]), int(inv["message_id"]))
                    except Exception:
                        pass
                if rows:
                    cur.execute(
                        "UPDATE banka_stars_invoices SET cleaned_flag = TRUE WHERE order_uid = %s;",
                        (order_uid,),
                    )
    except Exception:
        pass


def send_stars_invoice_for_order(chat_id: int, order: dict[str, Any]) -> None:
    order_uid = int(order["order_uid"])
    stars_amt = int(order["stars_amount"])
    pay_code = str(order["pay_code"]).upper()
    title, desc = describe_order(order)

    # Закрываем старые счета по этому заказу, если они были
    delete_uncleaned_invoices_for_order(order_uid)

    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            text=f"⭐ Оплатить {stars_amt} Stars",
            pay=True,
        )
    )
    kb.add(
        types.InlineKeyboardButton(
            text="❌ Отменить оплату",
            callback_data=f"cancel_order:{order_uid}",
        )
    )

    msg = bot.send_invoice(
        chat_id=chat_id,
        title=title[:32],
        description=f"{desc}\nКод заказа: {pay_code}"[:255],
        invoice_payload=f"gdz_order:{order_uid}:{pay_code}",
        provider_token="",  # Обязательно пустая строка для Telegram Stars (XTR)
        currency="XTR",
        prices=[types.LabeledPrice(label=title[:32], amount=stars_amt)],
        reply_markup=kb,
    )

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO banka_stars_invoices (order_uid, chat_id, message_id, cleaned_flag, created_ms)
                    VALUES (%s, %s, %s, FALSE, %s);
                    """,
                    (order_uid, chat_id, msg.message_id, now_ms()),
                )
    except Exception:
        pass


def create_order_for_tg_buyer(
    gdz_user: dict[str, Any],
    tg_user_id: int,
    item_kind: str,
    plan_days: int = 0,
    coins_amount: int = 0,
    target_bg_id: int = 0,
    stars_amount: int = 1,
) -> dict[str, Any]:
    buyer_uid = int(gdz_user["rec_uid"])
    buyer_login = str(gdz_user.get("entry_tag") or "").strip()[:100]
    ts = now_ms()
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE banka_stars_orders
                SET status_tag = 'CANCELLED'
                WHERE buyer_uid = %s AND status_tag = 'PENDING'
                RETURNING order_uid;
                """,
                (buyer_uid,),
            )
            cancelled = cur.fetchall()

            inserted = None
            for _ in range(8):
                code = generate_pay_code()
                cur.execute(
                    """
                    INSERT INTO banka_stars_orders (
                        pay_code, buyer_uid, buyer_login, item_kind,
                        plan_days, coins_amount, target_bg_id, stars_amount,
                        status_tag, telegram_user_id, ui_celebrated, created_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, FALSE, %s)
                    ON CONFLICT (pay_code) DO NOTHING
                    RETURNING order_uid, pay_code, buyer_uid, buyer_login, item_kind,
                              plan_days, coins_amount, target_bg_id, stars_amount, status_tag;
                    """,
                    (
                        code,
                        buyer_uid,
                        buyer_login,
                        item_kind,
                        max(0, int(plan_days)),
                        max(0, int(coins_amount)),
                        max(0, int(target_bg_id)),
                        max(1, int(stars_amount)),
                        tg_user_id,
                        ts,
                    ),
                )
                row = cur.fetchone()
                if row:
                    inserted = row
                    break
            if not inserted:
                raise RuntimeError("Не удалось создать код заказа")
            if target_bg_id > 0:
                cur.execute(
                    "SELECT title_txt FROM banka_shop_backgrounds WHERE bg_uid = %s;",
                    (target_bg_id,),
                )
                bg = cur.fetchone()
                inserted["bg_title"] = bg["title_txt"] if bg else f"Фон #{target_bg_id}"

    for c in cancelled:
        delete_uncleaned_invoices_for_order(int(c["order_uid"]))
    return inserted


def main_menu_markup(linked_login: str = "") -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(
            text="⭐ Купить в боте (Premium / Монеты / Фоны)",
            callback_data="shop:menu",
        )
    )
    acc_text = (
        f"🔄 Сменить аккаунт GDZgo (@{linked_login})"
        if linked_login
        else "🔗 Привязать аккаунт GDZgo"
    )
    kb.add(types.InlineKeyboardButton(text=acc_text, callback_data="shop:link"))
    return kb


def find_gdz_user_by_login(login_text: str) -> dict[str, Any] | None:
    clean = login_text.strip().lstrip("@").lower()
    if not clean:
        return None
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rec_uid, entry_tag, title_first, title_last,
                       COALESCE(ban_flag, FALSE) AS ban_flag,
                       COALESCE(vip_flag, FALSE) AS vip_flag,
                       COALESCE(vault_tokens, 0) AS vault_tokens
                FROM banka_profiles_vault
                WHERE LOWER(entry_tag) = %s OR LOWER(chat_username) = %s
                LIMIT 1;
                """,
                (clean, clean),
            )
            return cur.fetchone()


def get_linked_gdz_user(tg_user_id: int) -> dict[str, Any] | None:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.rec_uid, u.entry_tag, u.title_first, u.title_last,
                       COALESCE(u.ban_flag, FALSE) AS ban_flag,
                       COALESCE(u.vip_flag, FALSE) AS vip_flag,
                       COALESCE(u.vault_tokens, 0) AS vault_tokens
                FROM banka_bot_users bu
                JOIN banka_profiles_vault u ON u.rec_uid = bu.linked_gdz_uid
                WHERE bu.tg_user_id = %s AND bu.linked_gdz_uid > 0
                LIMIT 1;
                """,
                (tg_user_id,),
            )
            return cur.fetchone()


def send_shop_categories(chat_id: int, gdz_user: dict[str, Any]) -> None:
    pricing = get_runtime_pricing()
    rate = pricing["coins_per_star"]
    disc_note = f" (скидка -{pricing['discount_pct']}%)" if pricing["discount_pct"] > 0 else ""
    text = (
        f"🛍 <b>Магазин GDZgo</b>\n"
        f"Аккаунт: <code>@{html.escape(str(gdz_user['entry_tag']))}</code>\n"
        f"Баланс монет: <b>{int(gdz_user['vault_tokens'])}</b>\n\n"
        f"Выберите товар для мгновенной оплаты в Telegram Stars{disc_note}:"
    )
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(
            text=f"👑 Premium 10 дней — ⭐ {pricing['stars_10d']}",
            callback_data="buy:prem10",
        ),
        types.InlineKeyboardButton(
            text=f"👑 Premium 30 дней — ⭐ {pricing['stars_30d']}",
            callback_data="buy:prem30",
        ),
        types.InlineKeyboardButton(
            text=f"🪙 Купить монеты (1⭐ ≈ {rate:g} монет)",
            callback_data="shop:coins",
        ),
        types.InlineKeyboardButton(
            text="🎨 Фоны профиля за ⭐",
            callback_data="shop:bgs",
        ),
        types.InlineKeyboardButton(
            text="🔄 Сменить аккаунт",
            callback_data="shop:link",
        ),
    )
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)


@bot.message_handler(commands=["start"])
def handle_start(message: types.Message) -> None:
    upsert_bot_user(message.from_user)
    parts = (message.text or "").strip().split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ""

    if arg.lower().startswith("pay_"):
        code = arg[4:].strip().upper()
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.order_uid, o.pay_code, o.buyer_uid, o.buyer_login,
                           o.item_kind, o.plan_days, o.coins_amount, o.target_bg_id,
                           o.stars_amount, o.status_tag,
                           COALESCE(b.title_txt, '') AS bg_title
                    FROM banka_stars_orders o
                    LEFT JOIN banka_shop_backgrounds b ON b.bg_uid = o.target_bg_id
                    WHERE UPPER(o.pay_code) = %s
                    LIMIT 1;
                    """,
                    (code,),
                )
                order = cur.fetchone()
                if order and order["status_tag"] == "PENDING" and message.from_user:
                    cur.execute(
                        "UPDATE banka_stars_orders SET telegram_user_id = %s WHERE order_uid = %s;",
                        (message.from_user.id, order["order_uid"]),
                    )
        if not order:
            bot.send_message(
                message.chat.id,
                f"Заказ с кодом <code>{html.escape(code)}</code> не найден.",
                parse_mode="HTML",
                reply_markup=main_menu_markup(),
            )
            return
        if order["status_tag"] == "PAID":
            bot.send_message(
                message.chat.id,
                f"✅ Заказ <code>{html.escape(code)}</code> уже оплачен!",
                parse_mode="HTML",
                reply_markup=main_menu_markup(str(order.get("buyer_login") or "")),
            )
            return
        if order["status_tag"] != "PENDING":
            bot.send_message(
                message.chat.id,
                f"❌ Заказ <code>{html.escape(code)}</code> отменён. Создайте новый счёт в приложении или в боте.",
                parse_mode="HTML",
                reply_markup=main_menu_markup(str(order.get("buyer_login") or "")),
            )
            return

        if message.from_user:
            upsert_bot_user(
                message.from_user,
                linked_uid=int(order["buyer_uid"]),
                linked_login=str(order.get("buyer_login") or ""),
            )
        send_stars_invoice_for_order(message.chat.id, order)
        return

    linked = get_linked_gdz_user(message.from_user.id) if message.from_user else None
    linked_login = str(linked["entry_tag"]) if linked else ""
    bot.send_message(
        message.chat.id,
        "⭐ <b>Официальный бот оплаты GDZgo</b>\n\n"
        "• Нажмите «Оплатить в Telegram Stars» в приложении GDZgo — и счёт откроется здесь автоматически.\n"
        "• Либо оформляйте покупки прямо в этом боте через меню ниже.",
        parse_mode="HTML",
        reply_markup=main_menu_markup(linked_login),
    )


@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call: types.CallbackQuery) -> None:
    upsert_bot_user(call.from_user)
    data = (call.data or "").strip()
    chat_id = call.message.chat.id if call.message else call.from_user.id

    if data.startswith("cancel_order:"):
        try:
            order_uid = int(data.split(":", 1)[1])
        except ValueError:
            bot.answer_callback_query(call.id, "Некорректный заказ")
            return
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE banka_stars_orders
                    SET status_tag = 'CANCELLED'
                    WHERE order_uid = %s AND status_tag = 'PENDING'
                    RETURNING order_uid;
                    """,
                    (order_uid,),
                )
                cancelled = cur.fetchone()
        delete_uncleaned_invoices_for_order(order_uid)
        if call.message:
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
        if cancelled:
            bot.answer_callback_query(call.id, "Заказ и счёт отменены")
            bot.send_message(chat_id, "❌ Оплата отменена, счёт в боте аннулирован.")
        else:
            bot.answer_callback_query(call.id, "Счёт уже закрыт")
        return

    if data == "shop:link":
        with state_lock:
            user_states[call.from_user.id] = {"step": "WAIT_LOGIN"}
        bot.answer_callback_query(call.id)
        bot.send_message(
            chat_id,
            "Введите ваш логин (<code>@login</code>) из приложения GDZgo:",
            parse_mode="HTML",
        )
        return

    if data == "shop:menu":
        bot.answer_callback_query(call.id)
        gdz_user = get_linked_gdz_user(call.from_user.id)
        if not gdz_user:
            with state_lock:
                user_states[call.from_user.id] = {"step": "WAIT_LOGIN"}
            bot.send_message(
                chat_id,
                "Сначала укажите ваш логин (<code>@login</code>) из приложения GDZgo:",
                parse_mode="HTML",
            )
            return
        if gdz_user["ban_flag"]:
            bot.send_message(chat_id, "⛔ Этот аккаунт GDZgo заблокирован.")
            return
        send_shop_categories(chat_id, gdz_user)
        return

    gdz_user = get_linked_gdz_user(call.from_user.id)
    if not gdz_user:
        bot.answer_callback_query(call.id, "Сначала привяжите аккаунт GDZgo", show_alert=True)
        return
    if gdz_user["ban_flag"]:
        bot.answer_callback_query(call.id, "Аккаунт заблокирован", show_alert=True)
        return

    pricing = get_runtime_pricing()

    if data == "buy:prem10":
        bot.answer_callback_query(call.id)
        order = create_order_for_tg_buyer(
            gdz_user,
            call.from_user.id,
            "PREMIUM_10",
            plan_days=10,
            stars_amount=pricing["stars_10d"],
        )
        send_stars_invoice_for_order(chat_id, order)
        return

    if data == "buy:prem30":
        bot.answer_callback_query(call.id)
        order = create_order_for_tg_buyer(
            gdz_user,
            call.from_user.id,
            "PREMIUM_30",
            plan_days=30,
            stars_amount=pricing["stars_30d"],
        )
        send_stars_invoice_for_order(chat_id, order)
        return

    if data == "shop:coins":
        bot.answer_callback_query(call.id)
        kb = types.InlineKeyboardMarkup(row_width=2)
        for s_amt in (10, 25, 50, 100, 250):
            coins = max(1, int(s_amt * pricing["coins_per_star"] + 0.5))
            kb.add(
                types.InlineKeyboardButton(
                    text=f"⭐ {s_amt} → 🪙 {coins}",
                    callback_data=f"buy:coins:{s_amt}",
                )
            )
        kb.add(types.InlineKeyboardButton(text="✏️ Ввести свою сумму ⭐", callback_data="shop:coins_custom"))
        kb.add(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="shop:menu"))
        bot.send_message(
            chat_id,
            f"🪙 <b>Покупка монет</b>\nКурс: 1 ⭐ ≈ {pricing['coins_per_star']:g} монет.",
            parse_mode="HTML",
            reply_markup=kb,
        )
        return

    if data == "shop:coins_custom":
        bot.answer_callback_query(call.id)
        with state_lock:
            user_states[call.from_user.id] = {"step": "WAIT_COINS_STARS"}
        bot.send_message(chat_id, "Введите количество Stars ⭐ (целое число больше 0):")
        return

    if data.startswith("buy:coins:"):
        try:
            s_amt = max(1, int(data.split(":")[2]))
        except ValueError:
            bot.answer_callback_query(call.id, "Ошибка суммы")
            return
        bot.answer_callback_query(call.id)
        coins = max(1, int(s_amt * pricing["coins_per_star"] + 0.5))
        order = create_order_for_tg_buyer(
            gdz_user,
            call.from_user.id,
            "COINS",
            coins_amount=coins,
            stars_amount=s_amt,
        )
        send_stars_invoice_for_order(chat_id, order)
        return

    if data == "shop:bgs":
        bot.answer_callback_query(call.id)
        ts = now_ms()
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.bg_uid, b.title_txt, COALESCE(b.price_stars, 0) AS price_stars,
                           COALESCE(b.discount_pct, 0) AS discount_pct,
                           COALESCE(b.is_limited, FALSE) AS is_limited,
                           b.max_qty, COALESCE(b.sold_count, 0) AS sold_count,
                           b.expires_ms, b.publish_at_ms
                    FROM banka_shop_backgrounds b
                    WHERE COALESCE(b.is_active, TRUE) = TRUE
                      AND COALESCE(b.price_stars, 0) > 0
                      AND (b.publish_at_ms IS NULL OR b.publish_at_ms <= %s)
                      AND NOT EXISTS (
                          SELECT 1 FROM banka_user_backgrounds ub
                          WHERE ub.owner_uid = %s AND ub.bg_uid = b.bg_uid
                      )
                    ORDER BY b.bg_uid DESC
                    LIMIT 25;
                    """,
                    (ts, gdz_user["rec_uid"]),
                )
                bgs = cur.fetchall()

        kb = types.InlineKeyboardMarkup(row_width=1)
        count_added = 0
        for bg in bgs:
            is_lim = bool(bg["is_limited"])
            if is_lim and bg["expires_ms"] is not None and ts >= int(bg["expires_ms"]):
                continue
            if is_lim and bg["max_qty"] is not None and int(bg["max_qty"]) > 0 and int(bg["sold_count"]) >= int(bg["max_qty"]):
                continue
            raw_s = int(bg["price_stars"])
            disc = max(0, min(99, int(bg["discount_pct"])))
            eff_s = max(1, int(math.ceil(raw_s * (100 - disc) / 100.0))) if disc > 0 else raw_s
            tag = " [Лимит]" if is_lim else ""
            kb.add(
                types.InlineKeyboardButton(
                    text=f"🎨 {bg['title_txt']}{tag} — ⭐ {eff_s}",
                    callback_data=f"buy:bg:{bg['bg_uid']}",
                )
            )
            count_added += 1

        kb.add(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="shop:menu"))
        if count_added == 0:
            bot.send_message(chat_id, "Нет доступных для покупки фонов за ⭐ (возможно, все уже куплены).", reply_markup=kb)
        else:
            bot.send_message(chat_id, "🎨 <b>Доступные фоны за Stars ⭐:</b>", parse_mode="HTML", reply_markup=kb)
        return

    if data.startswith("buy:bg:"):
        try:
            bg_uid = int(data.split(":")[2])
        except ValueError:
            bot.answer_callback_query(call.id, "Некорректный фон")
            return
        ts = now_ms()
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT bg_uid, title_txt, COALESCE(price_stars, 0) AS price_stars,
                           COALESCE(discount_pct, 0) AS discount_pct,
                           COALESCE(is_limited, FALSE) AS is_limited,
                           max_qty, COALESCE(sold_count, 0) AS sold_count,
                           expires_ms, publish_at_ms, COALESCE(is_active, TRUE) AS is_active
                    FROM banka_shop_backgrounds
                    WHERE bg_uid = %s;
                    """,
                    (bg_uid,),
                )
                bg = cur.fetchone()
                cur.execute(
                    "SELECT 1 FROM banka_user_backgrounds WHERE owner_uid = %s AND bg_uid = %s;",
                    (gdz_user["rec_uid"], bg_uid),
                )
                owned = cur.fetchone() is not None
        if not bg or not bg["is_active"] or int(bg["price_stars"]) <= 0:
            bot.answer_callback_query(call.id, "Фон недоступен", show_alert=True)
            return
        if owned:
            bot.answer_callback_query(call.id, "Этот фон уже у вас в коллекции", show_alert=True)
            return
        if bg["publish_at_ms"] is not None and int(bg["publish_at_ms"]) > ts:
            bot.answer_callback_query(call.id, "Фон ещё не вышел в продажу", show_alert=True)
            return
        if bg["is_limited"]:
            if bg["expires_ms"] is not None and ts >= int(bg["expires_ms"]):
                bot.answer_callback_query(call.id, "Срок продажи лимитного фона истёк", show_alert=True)
                return
            if bg["max_qty"] is not None and int(bg["max_qty"]) > 0 and int(bg["sold_count"]) >= int(bg["max_qty"]):
                bot.answer_callback_query(call.id, "Тираж лимитного фона раскуплен", show_alert=True)
                return
        disc = max(0, min(99, int(bg["discount_pct"])))
        eff_s = max(1, int(math.ceil(int(bg["price_stars"]) * (100 - disc) / 100.0))) if disc > 0 else int(bg["price_stars"])
        bot.answer_callback_query(call.id)
        order = create_order_for_tg_buyer(
            gdz_user,
            call.from_user.id,
            "BACKGROUND",
            target_bg_id=bg_uid,
            stars_amount=eff_s,
        )
        send_stars_invoice_for_order(chat_id, order)
        return

    bot.answer_callback_query(call.id)


@bot.message_handler(content_types=["text"])
def handle_text(message: types.Message) -> None:
    upsert_bot_user(message.from_user)
    if not message.from_user:
        return
    uid = message.from_user.id
    text = (message.text or "").strip()

    with state_lock:
        st = user_states.get(uid)

    if st and st.get("step") == "WAIT_LOGIN":
        gdz_user = find_gdz_user_by_login(text)
        if not gdz_user:
            bot.send_message(
                message.chat.id,
                "Пользователь с таким логином в GDZgo не найден. Проверьте логин и попробуйте ещё раз:",
            )
            return
        if gdz_user["ban_flag"]:
            with state_lock:
                user_states.pop(uid, None)
            bot.send_message(message.chat.id, "⛔ Этот аккаунт заблокирован в GDZgo.")
            return
        with state_lock:
            user_states.pop(uid, None)
        upsert_bot_user(
            message.from_user,
            linked_uid=int(gdz_user["rec_uid"]),
            linked_login=str(gdz_user["entry_tag"]),
        )
        send_shop_categories(message.chat.id, gdz_user)
        return

    if st and st.get("step") == "WAIT_COINS_STARS":
        try:
            s_amt = int(text)
        except ValueError:
            bot.send_message(message.chat.id, "Введите целое число звёзд ⭐, например: 25")
            return
        if s_amt <= 0 or s_amt > 100000:
            bot.send_message(message.chat.id, "Укажите сумму от 1 до 100000 ⭐:")
            return
        with state_lock:
            user_states.pop(uid, None)
        gdz_user = get_linked_gdz_user(uid)
        if not gdz_user:
            bot.send_message(message.chat.id, "Сначала привяжите аккаунт GDZgo через /start")
            return
        pricing = get_runtime_pricing()
        coins = max(1, int(s_amt * pricing["coins_per_star"] + 0.5))
        order = create_order_for_tg_buyer(
            gdz_user,
            uid,
            "COINS",
            coins_amount=coins,
            stars_amount=s_amt,
        )
        send_stars_invoice_for_order(message.chat.id, order)
        return

    # Если человек просто ввёл 6-символьный pay_code текстом
    clean_code = text.upper()
    if len(clean_code) == 6 and clean_code.isalnum():
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.order_uid, o.pay_code, o.buyer_uid, o.buyer_login,
                           o.item_kind, o.plan_days, o.coins_amount, o.target_bg_id,
                           o.stars_amount, o.status_tag,
                           COALESCE(b.title_txt, '') AS bg_title
                    FROM banka_stars_orders o
                    LEFT JOIN banka_shop_backgrounds b ON b.bg_uid = o.target_bg_id
                    WHERE UPPER(o.pay_code) = %s AND o.status_tag = 'PENDING'
                    LIMIT 1;
                    """,
                    (clean_code,),
                )
                order = cur.fetchone()
        if order:
            upsert_bot_user(
                message.from_user,
                linked_uid=int(order["buyer_uid"]),
                linked_login=str(order.get("buyer_login") or ""),
            )
            send_stars_invoice_for_order(message.chat.id, order)
            return

    linked = get_linked_gdz_user(uid)
    bot.send_message(
        message.chat.id,
        "Используйте кнопки меню или перейдите по кнопке оплаты в приложении GDZgo.",
        reply_markup=main_menu_markup(str(linked["entry_tag"]) if linked else ""),
    )


@bot.pre_checkout_query_handler(func=lambda q: True)
def handle_pre_checkout(query: types.PreCheckoutQuery) -> None:
    upsert_bot_user(query.from_user)
    payload = (query.invoice_payload or "").strip()
    if not payload.startswith("gdz_order:") or query.currency != "XTR":
        bot.answer_pre_checkout_query(query.id, ok=False, error_message="Некорректный счёт.")
        return
    try:
        _, oid_str, code = payload.split(":", 2)
        order_uid = int(oid_str)
    except Exception:
        bot.answer_pre_checkout_query(query.id, ok=False, error_message="Некорректный код заказа.")
        return

    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_uid, pay_code, stars_amount, status_tag
                    FROM banka_stars_orders
                    WHERE order_uid = %s;
                    """,
                    (order_uid,),
                )
                order = cur.fetchone()
        if not order or str(order["pay_code"]).upper() != code.upper():
            bot.answer_pre_checkout_query(query.id, ok=False, error_message="Заказ не найден.")
            return
        if order["status_tag"] != "PENDING":
            delete_uncleaned_invoices_for_order(order_uid)
            bot.answer_pre_checkout_query(
                query.id,
                ok=False,
                error_message="Оплата отменена или заказ уже закрыт. Деньги не списаны.",
            )
            return
        if int(order["stars_amount"]) != int(query.total_amount):
            bot.answer_pre_checkout_query(
                query.id,
                ok=False,
                error_message="Сумма заказа изменилась. Создайте новый счёт.",
            )
            return
        bot.answer_pre_checkout_query(query.id, ok=True)
    except Exception:
        bot.answer_pre_checkout_query(
            query.id,
            ok=False,
            error_message="Временная ошибка базы данных, повторите попытку.",
        )


@bot.message_handler(content_types=["successful_payment"])
def handle_successful_payment(message: types.Message) -> None:
    upsert_bot_user(message.from_user)
    sp = message.successful_payment
    if not sp or not message.from_user:
        return
    tg_uid = message.from_user.id
    charge_id = sp.telegram_payment_charge_id
    payload = (sp.invoice_payload or "").strip()

    try:
        _, oid_str, _ = payload.split(":", 2)
        order_uid = int(oid_str)
    except Exception:
        return

    need_refund = False
    summary_text = ""

    with db_connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_uid, pay_code, buyer_uid, buyer_login,
                           item_kind, plan_days, coins_amount, target_bg_id,
                           stars_amount, status_tag, telegram_charge_id
                    FROM banka_stars_orders
                    WHERE order_uid = %s
                    FOR UPDATE;
                    """,
                    (order_uid,),
                )
                order = cur.fetchone()
                if not order:
                    need_refund = True
                elif order.get("telegram_charge_id") == charge_id:
                    return
                elif order["status_tag"] != "PENDING":
                    need_refund = True
                else:
                    paid_ms = now_ms()
                    cur.execute(
                        """
                        UPDATE banka_stars_orders
                        SET status_tag = 'PAID',
                            telegram_user_id = %s,
                            telegram_charge_id = %s,
                            paid_ms = %s
                        WHERE order_uid = %s;
                        """,
                        (tg_uid, charge_id, paid_ms, order_uid),
                    )
                    buyer_uid = int(order["buyer_uid"])
                    stars_amt = int(order["stars_amount"])
                    kind = str(order["item_kind"]).upper()

                    if kind == "COINS":
                        coins = max(1, int(order["coins_amount"]))
                        cur.execute(
                            "UPDATE banka_profiles_vault SET vault_tokens = vault_tokens + %s WHERE rec_uid = %s;",
                            (coins, buyer_uid),
                        )
                        insert_user_notice(
                            cur,
                            buyer_uid,
                            "COIN_PURCHASE_REWARD",
                            f"+{coins} монет за ⭐ {stars_amt}!",
                            f"Оплата через Telegram Stars выполнена. Начислено {coins} монет.",
                        )
                        summary_text = f"🪙 Начислено <b>+{coins} монет</b>!"
                    elif kind == "BACKGROUND":
                        bg_uid = int(order["target_bg_id"])
                        cur.execute(
                            "SELECT title_txt FROM banka_shop_backgrounds WHERE bg_uid = %s FOR UPDATE;",
                            (bg_uid,),
                        )
                        bg_row = cur.fetchone()
                        bg_title = bg_row["title_txt"] if bg_row else f"Фон #{bg_uid}"
                        cur.execute(
                            """
                            INSERT INTO banka_user_backgrounds (owner_uid, bg_uid, granted_via)
                            VALUES (%s, %s, 'STARS')
                            ON CONFLICT (owner_uid, bg_uid) DO NOTHING
                            RETURNING bg_uid;
                            """,
                            (buyer_uid, bg_uid),
                        )
                        if cur.fetchone():
                            cur.execute(
                                """
                                UPDATE banka_shop_backgrounds
                                SET sold_count = COALESCE(sold_count, 0) + 1,
                                    earned_stars = COALESCE(earned_stars, 0) + %s
                                WHERE bg_uid = %s;
                                """,
                                (stars_amt, bg_uid),
                            )
                        insert_user_notice(
                            cur,
                            buyer_uid,
                            "COIN_PURCHASE_REWARD",
                            f"Фон «{bg_title}» куплен за ⭐ {stars_amt}!",
                            f"Вы приобрели фон «{bg_title}» через Telegram Stars. Он уже доступен в коллекции.",
                        )
                        summary_text = f"🎨 Фон «<b>{html.escape(str(bg_title))}</b>» добавлен в вашу коллекцию!"
                    else:
                        days = max(1, int(order["plan_days"] or 30))
                        grant_timed_premium_in_tx(cur, buyer_uid, days, stars_amt)
                        summary_text = f"👑 Статус <b>Premium на {days} дн.</b> активирован!"

    if need_refund:
        try:
            bot.refund_star_payment(tg_uid, charge_id)
        except Exception:
            pass
        bot.send_message(
            message.chat.id,
            "⚠️ Этот заказ уже был отменён ранее. Звёзды возвращены на ваш баланс Telegram.",
        )
        return

    upsert_bot_user(
        message.from_user,
        linked_uid=int(order["buyer_uid"]),
        linked_login=str(order.get("buyer_login") or ""),
    )
    delete_uncleaned_invoices_for_order(order_uid)
    bot.send_message(
        message.chat.id,
        f"✅ <b>Оплата прошла успешно!</b>\n{summary_text}\n"
        f"Аккаунт GDZgo: <code>@{html.escape(str(order.get('buyer_login') or ''))}</code>",
        parse_mode="HTML",
        reply_markup=main_menu_markup(str(order.get("buyer_login") or "")),
    )


def cancelled_invoice_watcher() -> None:
    while True:
        try:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT i.invoice_uid, i.chat_id, i.message_id, o.status_tag
                        FROM banka_stars_invoices i
                        JOIN banka_stars_orders o ON o.order_uid = i.order_uid
                        WHERE i.cleaned_flag = FALSE AND o.status_tag <> 'PENDING'
                        LIMIT 30;
                        """
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        try:
                            bot.delete_message(int(r["chat_id"]), int(r["message_id"]))
                        except Exception:
                            pass
                        cur.execute(
                            "UPDATE banka_stars_invoices SET cleaned_flag = TRUE WHERE invoice_uid = %s;",
                            (r["invoice_uid"],),
                        )
        except Exception:
            pass
        time.sleep(2.0)


# -----------------------------
# Веб-портал & Админ-панель (/, /admin, /api/*)
# -----------------------------
def serialize_admin(row: dict[str, Any]) -> dict[str, Any]:
    role = str(row.get("role") or ROLE_USER)
    perms = {k: True for k in ALL_PERMISSIONS} if role == ROLE_SUPER_ADMIN else safe_json_dict(row.get("perms_json"))
    return {
        "web_uid": int(row["web_uid"]),
        "login": str(row["login"]),
        "display_name": str(row.get("display_name") or row["login"]),
        "role": role,
        "permissions": perms,
        "created_ms": int(row.get("created_ms") or 0),
        "last_login_ms": int(row.get("last_login_ms") or 0),
    }


def has_perm(user: dict[str, Any] | None, perm: str | None = None) -> bool:
    if not user:
        return False
    role = user["role"]
    if role == ROLE_SUPER_ADMIN:
        return True
    if role != ROLE_MODERATOR:
        return False
    if perm is None:
        return True
    return bool(user["permissions"].get(perm))


HTML_PAGE = r"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>GDZgo Bot — Портал и Админ-панель</title>
<style>
  :root {
    --bg: #070b14;
    --panel: rgba(17, 25, 42, 0.88);
    --border: rgba(148, 163, 184, 0.18);
    --text: #f1f5f9;
    --muted: #94a3b8;
    --accent: #38bdf8;
    --accent2: #818cf8;
    --ok: #34d399;
    --warn: #fbbf24;
    --err: #fb7185;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: radial-gradient(circle at 15% 10%, #102142 0%, var(--bg) 55%);
    color: var(--text);
    min-height: 100vh;
  }
  .wrap { max-width: 1180px; margin: 0 auto; padding: 20px 16px 48px; }
  .topbar {
    display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between;
    gap: 12px; padding: 14px 18px; border-radius: 18px;
    background: var(--panel); border: 1px solid var(--border); backdrop-filter: blur(12px);
    margin-bottom: 18px;
  }
  .brand { display: flex; align-items: center; gap: 10px; font-weight: 700; font-size: 18px; }
  .pill {
    font-size: 12px; padding: 4px 10px; border-radius: 999px;
    background: rgba(56,189,248,0.16); color: var(--accent); border: 1px solid rgba(56,189,248,0.35);
  }
  .pill.super { background: rgba(251,191,36,0.18); color: var(--warn); border-color: rgba(251,191,36,0.4); }
  .pill.mod { background: rgba(129,140,248,0.2); color: #c7d2fe; border-color: rgba(129,140,248,0.4); }
  .pill.user { background: rgba(251,113,133,0.16); color: var(--err); border-color: rgba(251,113,133,0.35); }
  .card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 18px;
    padding: 18px; margin-bottom: 16px; backdrop-filter: blur(10px);
  }
  .auth-box { max-width: 430px; margin: 40px auto; }
  .tabs { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
  .tab {
    padding: 9px 14px; border-radius: 12px; border: 1px solid var(--border);
    background: rgba(15,23,42,0.7); color: var(--muted); cursor: pointer; font-weight: 600; font-size: 14px;
  }
  .tab.active {
    background: linear-gradient(135deg, rgba(56,189,248,0.24), rgba(129,140,248,0.24));
    color: #fff; border-color: rgba(56,189,248,0.5);
  }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; }
  .kpi {
    padding: 15px; border-radius: 15px; background: rgba(15, 23, 42, 0.72);
    border: 1px solid var(--border);
  }
  .kpi .lbl { color: var(--muted); font-size: 12px; margin-bottom: 6px; }
  .kpi .val { font-size: 24px; font-weight: 800; color: #fff; }
  .kpi .sub { color: var(--ok); font-size: 12px; margin-top: 4px; }
  input, textarea, select {
    width: 100%; padding: 10px 12px; border-radius: 11px;
    border: 1px solid var(--border); background: rgba(8, 13, 25, 0.9);
    color: var(--text); font-size: 14px; margin-top: 6px; margin-bottom: 12px;
  }
  textarea { min-height: 96px; resize: vertical; font-family: inherit; }
  label { font-size: 13px; color: var(--muted); display: block; }
  button {
    padding: 10px 16px; border-radius: 11px; border: 0; cursor: pointer;
    font-weight: 600; font-size: 14px; color: #04101d;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
  }
  button.sec {
    background: rgba(148,163,184,0.16); color: var(--text);
    border: 1px solid var(--border);
  }
  button.danger { background: rgba(251,113,133,0.2); color: #fecdd3; border: 1px solid rgba(251,113,133,0.45); }
  button:disabled { opacity: 0.55; cursor: not-allowed; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(148,163,184,0.12); vertical-align: top; }
  th { color: var(--muted); font-weight: 600; }
  .table-wrap { overflow-x: auto; }
  .msg { padding: 10px 12px; border-radius: 12px; margin-bottom: 12px; font-size: 13px; }
  .msg.ok { background: rgba(52,211,153,0.15); border: 1px solid rgba(52,211,153,0.4); color: #a7f3d0; }
  .msg.err { background: rgba(251,113,133,0.15); border: 1px solid rgba(251,113,133,0.4); color: #fecdd3; }
  .muted { color: var(--muted); font-size: 13px; }
  .row-actions { display: flex; flex-wrap: wrap; gap: 6px; }
  .perm-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 6px; margin-top: 8px; }
  .perm-item {
    display: flex; align-items: center; gap: 8px; padding: 6px 10px;
    border-radius: 10px; background: rgba(15,23,42,0.7); border: 1px solid var(--border);
    font-size: 12px; cursor: pointer;
  }
  .perm-item input { width: auto; margin: 0; }
  .modal-bg {
    position: fixed; inset: 0; background: rgba(3,7,18,0.75);
    display: none; align-items: center; justify-content: center; padding: 16px; z-index: 50;
  }
  .modal-bg.open { display: flex; }
  .modal {
    width: 100%; max-width: 780px; max-height: 88vh; overflow-y: auto;
    background: #0d1526; border: 1px solid var(--border); border-radius: 18px; padding: 20px;
  }
  .hide { display: none !important; }
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="brand">
      <span>⭐ GDZgo Bot</span>
      <span id="routeBadge" class="pill">Портал</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <a id="switchAdminLink" href="/admin" class="pill" style="text-decoration:none;">Перейти в /admin</a>
      <span id="userBadge" class="pill hide"></span>
      <button id="logoutBtn" class="sec hide" onclick="doLogout()">Выйти</button>
    </div>
  </div>

  <!-- AUTH VIEW -->
  <div id="authCard" class="card auth-box hide">
    <h2 id="authTitle" style="margin-top:0;">Вход</h2>
    <p id="bootstrapHint" class="msg ok hide"></p>
    <div class="tabs">
      <button id="tabLogin" class="tab active" onclick="setAuthMode('login')">Вход</button>
      <button id="tabReg" class="tab" onclick="setAuthMode('register')">Регистрация</button>
    </div>
    <div id="authMsg"></div>
    <label>Логин</label>
    <input id="authLogin" placeholder="например: admin" autocomplete="username" />
    <div id="regNameWrap" class="hide">
      <label>Имя (для отображения)</label>
      <input id="authName" placeholder="Ваше имя" />
    </div>
    <label>Пароль</label>
    <input id="authPass" type="password" placeholder="Минимум 6 символов" autocomplete="current-password" />
    <button id="authSubmitBtn" style="width:100%;" onclick="submitAuth()">Войти</button>
  </div>

  <!-- NOT ADMIN / NOT MODERATOR -->
  <div id="deniedCard" class="card auth-box hide" style="text-align:center;">
    <h2 style="color:var(--err);margin-top:0;">Доступ ограничен</h2>
    <p style="font-size:16px;font-weight:600;">Вы не админ и не модератор</p>
    <p class="muted">Ваш аккаунт зарегистрирован в системе. Попросите Супер-Администратора выдать вам роль модератора и права доступа.</p>
    <button class="sec" onclick="doLogout()">Выйти из аккаунта</button>
  </div>

  <!-- ADMIN / MODERATOR PANEL -->
  <div id="panelView" class="hide">
    <div id="navTabs" class="tabs"></div>
    <div id="panelAlert"></div>

    <div id="secStats" class="hide"></div>
    <div id="secUsers" class="hide"></div>
    <div id="secOrders" class="hide"></div>
    <div id="secMessages" class="hide"></div>
    <div id="secAdmins" class="hide"></div>
  </div>
</div>

<!-- USER DETAIL MODAL -->
<div id="userModalBg" class="modal-bg" onclick="if(event.target===this) closeModal()">
  <div class="modal">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <h3 id="modalTitle" style="margin:0;">Профиль пользователя бота</h3>
      <button class="sec" onclick="closeModal()">Закрыть</button>
    </div>
    <div id="modalBody"></div>
  </div>
</div>

<script>
const PERM_LABELS = {
  view_stats: "Доходы и аналитика",
  view_users: "Пользователи и история ⭐",
  view_orders: "Все заказы Stars",
  send_dm: "Личные сообщения (ЛС)",
  send_broadcast: "Массовая рассылка",
  manage_orders: "Отмена PENDING-заказов"
};

let authToken = sessionStorage.getItem("gdz_bot_token") || "";
let currentUser = null;
let authMode = "login";
let activeTab = "";
let cachedUsers = [];
const isAdminPath = window.location.pathname.startsWith("/admin");

document.getElementById("routeBadge").textContent = isAdminPath ? "Админ-панель (/admin)" : "Портал пользователей (/)";
if (isAdminPath) {
  document.getElementById("switchAdminLink").href = "/";
  document.getElementById("switchAdminLink").textContent = "На главную (/)";
}

function el(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined && text !== null) n.textContent = String(text);
  if (cls) n.className = cls;
  return n;
}

function showMsg(containerId, text, isErr) {
  const box = document.getElementById(containerId);
  box.innerHTML = "";
  if (!text) return;
  box.appendChild(el("div", text, "msg " + (isErr ? "err" : "ok")));
}

function fmtTime(ms) {
  if (!ms) return "—";
  const d = new Date(Number(ms) + 3 * 3600 * 1000);
  const pad = n => String(n).padStart(2, "0");
  return `${pad(d.getUTCDate())}.${pad(d.getUTCMonth()+1)}.${d.getUTCFullYear()} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} МСК`;
}

async function api(path, opts = {}) {
  const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
  if (authToken) headers["Authorization"] = "Bearer " + authToken;
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  let data = {};
  try { data = await res.json(); } catch (_) {}
  if (!res.ok) throw new Error(data.error || ("Ошибка HTTP " + res.status));
  return data;
}

function setAuthMode(mode) {
  authMode = mode;
  document.getElementById("tabLogin").classList.toggle("active", mode === "login");
  document.getElementById("tabReg").classList.toggle("active", mode === "register");
  document.getElementById("regNameWrap").classList.toggle("hide", mode !== "register");
  document.getElementById("authTitle").textContent = mode === "register" ? "Регистрация аккаунта" : (isAdminPath ? "Вход в админ-панель" : "Вход в систему");
  document.getElementById("authSubmitBtn").textContent = mode === "register" ? "Зарегистрироваться" : "Войти";
  showMsg("authMsg", "", false);
}

async function submitAuth() {
  const login = document.getElementById("authLogin").value.trim();
  const password = document.getElementById("authPass").value;
  const display_name = document.getElementById("authName").value.trim();
  try {
    const ep = authMode === "register" ? "/api/auth/register" : "/api/auth/login";
    const data = await api(ep, {
      method: "POST",
      body: JSON.stringify({ login, password, display_name })
    });
    authToken = data.token;
    sessionStorage.setItem("gdz_bot_token", authToken);
    currentUser = data.user;
    renderApp();
  } catch (e) {
    showMsg("authMsg", e.message, true);
  }
}

async function doLogout() {
  try { await api("/api/auth/logout", { method: "POST" }); } catch (_) {}
  authToken = "";
  currentUser = null;
  sessionStorage.removeItem("gdz_bot_token");
  await bootstrap();
}

function hasPerm(perm) {
  if (!currentUser) return false;
  if (currentUser.role === "SUPER_ADMIN") return true;
  if (currentUser.role !== "MODERATOR") return false;
  return !!(currentUser.permissions && currentUser.permissions[perm]);
}

async function bootstrap() {
  let st = { has_super_admin: true };
  try { st = await api("/api/auth/status"); } catch (_) {}
  const hint = document.getElementById("bootstrapHint");
  if (!st.has_super_admin) {
    hint.textContent = "База администраторов пуста: первый зарегистрированный аккаунт станет Супер-Администратором!";
    hint.classList.remove("hide");
    if (!isAdminPath) setAuthMode("register");
  } else {
    hint.classList.add("hide");
  }

  if (authToken) {
    try {
      const me = await api("/api/auth/me");
      currentUser = me.user;
    } catch (_) {
      authToken = "";
      currentUser = null;
      sessionStorage.removeItem("gdz_bot_token");
    }
  }
  renderApp();
}

function renderApp() {
  const authCard = document.getElementById("authCard");
  const deniedCard = document.getElementById("deniedCard");
  const panelView = document.getElementById("panelView");
  const badge = document.getElementById("userBadge");
  const logoutBtn = document.getElementById("logoutBtn");

  authCard.classList.add("hide");
  deniedCard.classList.add("hide");
  panelView.classList.add("hide");
  badge.classList.add("hide");
  logoutBtn.classList.add("hide");

  if (!currentUser) {
    authCard.classList.remove("hide");
    return;
  }

  badge.classList.remove("hide");
  logoutBtn.classList.remove("hide");
  badge.textContent = `${currentUser.display_name} (@${currentUser.login}) · ${currentUser.role}`;
  badge.className = "pill " + (
    currentUser.role === "SUPER_ADMIN" ? "super" :
    currentUser.role === "MODERATOR" ? "mod" : "user"
  );

  if (currentUser.role !== "SUPER_ADMIN" && currentUser.role !== "MODERATOR") {
    deniedCard.classList.remove("hide");
    return;
  }

  panelView.classList.remove("hide");
  buildTabs();
}

function buildTabs() {
  const tabs = [];
  if (hasPerm("view_stats")) tabs.push({ id: "stats", label: "📊 Доходы и обзор" });
  if (hasPerm("view_users")) tabs.push({ id: "users", label: "👥 Пользователи бота" });
  if (hasPerm("view_orders")) tabs.push({ id: "orders", label: "🧾 Заказы Stars" });
  if (hasPerm("send_dm") || hasPerm("send_broadcast")) tabs.push({ id: "messages", label: "✉️ Сообщения и рассылка" });
  if (currentUser.role === "SUPER_ADMIN") tabs.push({ id: "admins", label: "🛡 Модераторы и права" });

  const nav = document.getElementById("navTabs");
  nav.innerHTML = "";
  if (!tabs.length) {
    showMsg("panelAlert", "У вас роль Модератор, но пока не включено ни одно право. Обратитесь к Супер-Админу.", true);
    return;
  }
  showMsg("panelAlert", "", false);

  if (!tabs.some(t => t.id === activeTab)) activeTab = tabs[0].id;
  for (const t of tabs) {
    const b = el("button", t.label, "tab" + (t.id === activeTab ? " active" : ""));
    b.onclick = () => { activeTab = t.id; buildTabs(); };
    nav.appendChild(b);
  }
  loadActiveTab();
}

function hideAllSections() {
  ["secStats", "secUsers", "secOrders", "secMessages", "secAdmins"].forEach(id => {
    document.getElementById(id).classList.add("hide");
  });
}

async function loadActiveTab() {
  hideAllSections();
  try {
    if (activeTab === "stats") await renderStats();
    else if (activeTab === "users") await renderUsers();
    else if (activeTab === "orders") await renderOrders();
    else if (activeTab === "messages") await renderMessages();
    else if (activeTab === "admins") await renderAdmins();
  } catch (e) {
    showMsg("panelAlert", e.message, true);
  }
}

async function renderStats() {
  const box = document.getElementById("secStats");
  box.classList.remove("hide");
  box.innerHTML = "<div class='card'>Загрузка аналитики...</div>";
  const d = await api("/api/stats");
  box.innerHTML = "";

  const kpiCard = el("div", null, "card");
  kpiCard.appendChild(el("h3", "Доходы в Telegram Stars ⭐ и завершённые сделки", null));
  const grid = el("div", null, "grid");
  const blocks = [
    ["Сегодня (МСК)", `⭐ ${d.day.stars}`, `Сделок: ${d.day.deals}`],
    ["За неделю (7 дн.)", `⭐ ${d.week.stars}`, `Сделок: ${d.week.deals}`],
    ["За месяц (30 дн.)", `⭐ ${d.month.stars}`, `Сделок: ${d.month.deals}`],
    ["За всё время", `⭐ ${d.all.stars}`, `Завершено сделок: ${d.all.deals}`],
    ["Пользователей бота", `${d.bot_users_count}`, `Оплативших: ${d.paying_users_count}`],
    ["Активные / Отменённые", `${d.pending_count} / ${d.cancelled_count}`, `В ожидании / Отменено`],
  ];
  for (const [lbl, val, sub] of blocks) {
    const k = el("div", null, "kpi");
    k.appendChild(el("div", lbl, "lbl"));
    k.appendChild(el("div", val, "val"));
    k.appendChild(el("div", sub, "sub"));
    grid.appendChild(k);
  }
  kpiCard.appendChild(grid);
  box.appendChild(kpiCard);

  const brCard = el("div", null, "card");
  brCard.appendChild(el("h3", "Разбивка доходов по типам покупок", null));
  const tw = el("div", null, "table-wrap");
  const tbl = el("table");
  tbl.innerHTML = "<thead><tr><th>Категория</th><th>Завершено сделок</th><th>Заработано ⭐</th></tr></thead>";
  const tb = el("tbody");
  for (const row of (d.by_kind || [])) {
    const tr = el("tr");
    tr.appendChild(el("td", row.kind_label));
    tr.appendChild(el("td", row.deals));
    tr.appendChild(el("td", "⭐ " + row.stars));
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  tw.appendChild(tbl);
  brCard.appendChild(tw);
  box.appendChild(brCard);
}

async function renderUsers() {
  const box = document.getElementById("secUsers");
  box.classList.remove("hide");
  box.innerHTML = "<div class='card'>Загрузка пользователей бота...</div>";
  const d = await api("/api/users");
  cachedUsers = d.users || [];
  box.innerHTML = "";

  const card = el("div", null, "card");
  card.appendChild(el("h3", `Пользователи бота (${cachedUsers.length})`, null));
  const search = el("input");
  search.placeholder = "Поиск по нику в GDZgo, Telegram @username, имени или TG ID...";
  card.appendChild(search);

  const tw = el("div", null, "table-wrap");
  const tbl = el("table");
  tbl.innerHTML = "<thead><tr><th>Telegram</th><th>Аккаунт GDZgo</th><th>Потрачено ⭐</th><th>Сделок</th><th>Последний визит</th><th>Действия</th></tr></thead>";
  const tb = el("tbody");

  function drawRows(q) {
    tb.innerHTML = "";
    const needle = (q || "").trim().toLowerCase();
    for (const u of cachedUsers) {
      const hay = `${u.tg_user_id} ${u.tg_username} ${u.first_name} ${u.last_name} ${u.gdz_login} ${u.gdz_full_name}`.toLowerCase();
      if (needle && !hay.includes(needle)) continue;
      const tr = el("tr");
      const tgName = [u.first_name, u.last_name].filter(Boolean).join(" ") || "Без имени";
      const tgTd = el("td");
      tgTd.appendChild(el("div", tgName));
      tgTd.appendChild(el("div", (u.tg_username ? "@" + u.tg_username : "без @") + " · ID " + u.tg_user_id, "muted"));

      const gdzTd = el("td");
      gdzTd.appendChild(el("div", u.gdz_login ? ("@" + u.gdz_login) : "Не привязан"));
      if (u.gdz_full_name || u.gdz_class) {
        gdzTd.appendChild(el("div", `${u.gdz_full_name || ""} ${u.gdz_class ? "(" + u.gdz_class + ")" : ""}`.trim(), "muted"));
      }

      tr.appendChild(tgTd);
      tr.appendChild(gdzTd);
      tr.appendChild(el("td", "⭐ " + u.total_stars_spent));
      tr.appendChild(el("td", u.paid_orders_count));
      tr.appendChild(el("td", fmtTime(u.last_seen_ms)));

      const act = el("td", null, "row-actions");
      const btnHist = el("button", "История ⭐", "sec");
      btnHist.onclick = () => openUserModal(u);
      act.appendChild(btnHist);
      if (hasPerm("send_dm")) {
        const btnDm = el("button", "Написать");
        btnDm.onclick = () => jumpToDm(u.tg_user_id);
        act.appendChild(btnDm);
      }
      tr.appendChild(act);
      tb.appendChild(tr);
    }
  }

  search.oninput = () => drawRows(search.value);
  drawRows("");
  tbl.appendChild(tb);
  tw.appendChild(tbl);
  card.appendChild(tw);
  box.appendChild(card);
}

async function openUserModal(u) {
  document.getElementById("userModalBg").classList.add("open");
  document.getElementById("modalTitle").textContent = `Пользователь: ${u.tg_username ? "@" + u.tg_username : u.tg_user_id}`;
  const body = document.getElementById("modalBody");
  body.innerHTML = "<p class='muted'>Загрузка истории покупок...</p>";
  try {
    const d = await api(`/api/users/${u.tg_user_id}/orders`);
    body.innerHTML = "";
    const info = el("div", null, "card");
    info.appendChild(el("div", `Telegram ID: ${u.tg_user_id} | Ник: ${u.tg_username ? "@" + u.tg_username : "—"}`));
    info.appendChild(el("div", `Ник в GDZgo: ${u.gdz_login ? "@" + u.gdz_login : "Не привязан"} · Имя: ${u.gdz_full_name || "—"}`));
    info.appendChild(el("div", `Всего потрачено: ⭐ ${u.total_stars_spent} (${u.paid_orders_count} оплат)`));
    if (u.gdz_login) {
      info.appendChild(el("div", `Баланс монет: ${u.gdz_coins} · Premium: ${u.gdz_vip ? "Активен" : "Нет"}`, "muted"));
    }
    body.appendChild(info);

    const hTitle = el("h4", "Полная история покупок за Stars ⭐", null);
    body.appendChild(hTitle);

    const orders = d.orders || [];
    if (!orders.length) {
      body.appendChild(el("p", "Покупок пока не было.", "muted"));
      return;
    }
    const tw = el("div", null, "table-wrap");
    const tbl = el("table");
    tbl.innerHTML = "<thead><tr><th>Время (МСК)</th><th>Товар</th><th>Сумма</th><th>Код</th><th>Статус</th></tr></thead>";
    const tb = el("tbody");
    for (const o of orders) {
      const tr = el("tr");
      tr.appendChild(el("td", fmtTime(o.paid_ms || o.created_ms)));
      tr.appendChild(el("td", o.item_summary));
      tr.appendChild(el("td", "⭐ " + o.stars_amount));
      tr.appendChild(el("td", o.pay_code));
      tr.appendChild(el("td", o.status_tag));
      tb.appendChild(tr);
    }
    tbl.appendChild(tb);
    tw.appendChild(tbl);
    body.appendChild(tw);
  } catch (e) {
    body.innerHTML = "";
    body.appendChild(el("div", e.message, "msg err"));
  }
}

function closeModal() {
  document.getElementById("userModalBg").classList.remove("open");
}

function jumpToDm(tgUserId) {
  activeTab = "messages";
  buildTabs();
  setTimeout(() => {
    const inp = document.getElementById("dmTargetId");
    if (inp) inp.value = String(tgUserId);
  }, 150);
}

async function renderOrders() {
  const box = document.getElementById("secOrders");
  box.classList.remove("hide");
  box.innerHTML = "<div class='card'>Загрузка заказов...</div>";
  const d = await api("/api/orders");
  box.innerHTML = "";
  const card = el("div", null, "card");
  card.appendChild(el("h3", "Последние заказы Telegram Stars (до 150)", null));
  const tw = el("div", null, "table-wrap");
  const tbl = el("table");
  tbl.innerHTML = "<thead><tr><th>ID</th><th>Время (МСК)</th><th>GDZgo / TG</th><th>Товар</th><th>⭐</th><th>Статус</th><th></th></tr></thead>";
  const tb = el("tbody");
  for (const o of (d.orders || [])) {
    const tr = el("tr");
    tr.appendChild(el("td", "#" + o.order_uid));
    tr.appendChild(el("td", fmtTime(o.paid_ms || o.created_ms)));
    tr.appendChild(el("td", `@${o.buyer_login || "?"} (TG: ${o.telegram_user_id || 0})`));
    tr.appendChild(el("td", `${o.item_summary} [${o.pay_code}]`));
    tr.appendChild(el("td", "⭐ " + o.stars_amount));
    tr.appendChild(el("td", o.status_tag));
    const tdAct = el("td");
    if (o.status_tag === "PENDING" && hasPerm("manage_orders")) {
      const btn = el("button", "Отменить", "danger");
      btn.onclick = async () => {
        await api("/api/orders/cancel", { method: "POST", body: JSON.stringify({ order_uid: o.order_uid }) });
        await renderOrders();
      };
      tdAct.appendChild(btn);
    }
    tr.appendChild(tdAct);
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  tw.appendChild(tbl);
  card.appendChild(tw);
  box.appendChild(card);
}

async function renderMessages() {
  const box = document.getElementById("secMessages");
  box.classList.remove("hide");
  box.innerHTML = "<div class='card'>Загрузка...</div>";
  const [hist, usrResp] = await Promise.all([
    api("/api/messages/history"),
    hasPerm("view_users") ? api("/api/users") : Promise.resolve({ users: cachedUsers })
  ]);
  const usersList = usrResp.users || [];
  box.innerHTML = "";

  if (hasPerm("send_dm")) {
    const dmCard = el("div", null, "card");
    dmCard.appendChild(el("h3", "Личное сообщение конкретному пользователю", null));
    if (usersList.length) {
      const selLbl = el("label", "Быстрый выбор из пользователей бота:");
      const sel = el("select");
      const opt0 = el("option", "— Выберите или введите Telegram ID ниже —");
      opt0.value = "";
      sel.appendChild(opt0);
      for (const u of usersList) {
        const op = el("option", `${u.tg_username ? "@" + u.tg_username : u.first_name || "User"} (GDZgo: @${u.gdz_login || "—"}) · ID ${u.tg_user_id}`);
        op.value = String(u.tg_user_id);
        sel.appendChild(op);
      }
      sel.onchange = () => { if (sel.value) document.getElementById("dmTargetId").value = sel.value; };
      dmCard.appendChild(selLbl);
      dmCard.appendChild(sel);
    }
    dmCard.appendChild(el("label", "Telegram User ID:"));
    const tid = el("input"); tid.id = "dmTargetId"; tid.placeholder = "например: 123456789";
    dmCard.appendChild(tid);
    dmCard.appendChild(el("label", "Текст сообщения:"));
    const txt = el("textarea"); txt.id = "dmText"; txt.placeholder = "Введите сообщение пользователю...";
    dmCard.appendChild(txt);
    const dmRes = el("div"); dmRes.id = "dmRes";
    dmCard.appendChild(dmRes);
    const btn = el("button", "Отправить пользователю");
    btn.onclick = async () => {
      try {
        const res = await api("/api/messages/dm", {
          method: "POST",
          body: JSON.stringify({ tg_user_id: Number(tid.value.trim()), text: txt.value })
        });
        txt.value = "";
        showMsg("dmRes", res.message || "Отправлено!", false);
        await renderMessages();
      } catch (e) {
        showMsg("dmRes", e.message, true);
      }
    };
    dmCard.appendChild(btn);
    box.appendChild(dmCard);
  }

  if (hasPerm("send_broadcast")) {
    const bcCard = el("div", null, "card");
    bcCard.appendChild(el("h3", "Рассылка всем пользователям бота", null));
    bcCard.appendChild(el("p", "Сообщение получат все пользователи из базы бота (banka_bot_users).", "muted"));
    const bTxt = el("textarea"); bTxt.id = "bcText"; bTxt.placeholder = "Текст массовой рассылки...";
    bcCard.appendChild(bTxt);
    const bcRes = el("div"); bcRes.id = "bcRes";
    bcCard.appendChild(bcRes);
    const bBtn = el("button", "Запустить рассылку всем");
    bBtn.onclick = async () => {
      if (!confirm("Отправить это сообщение ВСЕМ пользователям бота?")) return;
      bBtn.disabled = true;
      try {
        const res = await api("/api/messages/broadcast", {
          method: "POST",
          body: JSON.stringify({ text: bTxt.value })
        });
        bTxt.value = "";
        showMsg("bcRes", `Рассылка завершена: доставлено ${res.sent}, ошибок ${res.failed}.`, false);
        await renderMessages();
      } catch (e) {
        showMsg("bcRes", e.message, true);
      } finally {
        bBtn.disabled = false;
      }
    };
    bcCard.appendChild(bBtn);
    box.appendChild(bcCard);
  }

  const hCard = el("div", null, "card");
  hCard.appendChild(el("h3", "Журнал отправленных сообщений и рассылок", null));
  const tw = el("div", null, "table-wrap");
  const tbl = el("table");
  tbl.innerHTML = "<thead><tr><th>Время (МСК)</th><th>Тип</th><th>Отправитель</th><th>Получатель / Охват</th><th>Текст</th></tr></thead>";
  const tb = el("tbody");
  for (const r of (hist.logs || [])) {
    const tr = el("tr");
    tr.appendChild(el("td", fmtTime(r.created_ms)));
    tr.appendChild(el("td", r.kind === "BROADCAST" ? "Рассылка" : "Личное"));
    tr.appendChild(el("td", "@" + r.sender_login));
    tr.appendChild(el("td", r.kind === "BROADCAST" ? `Доставлено: ${r.sent_count}, ошибок: ${r.failed_count}` : `TG ID ${r.target_tg_user_id}`));
    tr.appendChild(el("td", r.message_text));
    tb.appendChild(tr);
  }
  tbl.appendChild(tb);
  tw.appendChild(tbl);
  hCard.appendChild(tw);
  box.appendChild(hCard);
}

async function renderAdmins() {
  const box = document.getElementById("secAdmins");
  box.classList.remove("hide");
  box.innerHTML = "<div class='card'>Загрузка списка аккаунтов...</div>";
  const d = await api("/api/admins");
  box.innerHTML = "";

  const card = el("div", null, "card");
  card.appendChild(el("h3", "Управление модераторами и кастомными правами", null));
  card.appendChild(el("p", "Обычные пользователи после регистрации видят сообщение «Вы не админ и не модератор». Назначьте роль MODERATOR и отметьте нужные права.", "muted"));

  for (const acc of (d.accounts || [])) {
    const item = el("div", null, "kpi");
    item.style.marginBottom = "12px";

    const top = el("div");
    top.style.display = "flex";
    top.style.justifyContent = "space-between";
    top.style.flexWrap = "wrap";
    top.style.gap = "8px";
    top.appendChild(el("strong", `${acc.display_name} (@${acc.login}) — ${acc.role}`));
    top.appendChild(el("span", `Регистрация: ${fmtTime(acc.created_ms)}`, "muted"));
    item.appendChild(top);

    if (acc.role === "SUPER_ADMIN") {
      item.appendChild(el("div", "Супер-Администратор имеет все права и не может быть понижен.", "muted"));
      card.appendChild(item);
      continue;
    }

    const roleSel = el("select");
    roleSel.style.maxWidth = "260px";
    [["USER", "USER (Нет доступа)"], ["MODERATOR", "MODERATOR (Доступ по правам)"]].forEach(([rv, rl]) => {
      const op = el("option", rl);
      op.value = rv;
      if (acc.role === rv) op.selected = true;
      roleSel.appendChild(op);
    });
    item.appendChild(roleSel);

    const pGrid = el("div", null, "perm-grid");
    const checks = {};
    for (const [pk, pl] of Object.entries(PERM_LABELS)) {
      const lbl = el("label", null, "perm-item");
      const cb = el("input");
      cb.type = "checkbox";
      cb.checked = !!(acc.permissions && acc.permissions[pk]);
      checks[pk] = cb;
      lbl.appendChild(cb);
      lbl.appendChild(el("span", pl));
      pGrid.appendChild(lbl);
    }
    item.appendChild(pGrid);

    const saveRow = el("div", null, "row-actions");
    saveRow.style.marginTop = "10px";
    const saveBtn = el("button", "Сохранить роль и права");
    saveBtn.onclick = async () => {
      const perms = {};
      for (const k of Object.keys(checks)) perms[k] = checks[k].checked;
      await api("/api/admins/role", {
        method: "POST",
        body: JSON.stringify({ web_uid: acc.web_uid, role: roleSel.value, permissions: perms })
      });
      showMsg("panelAlert", `Права для @${acc.login} обновлены!`, false);
      await renderAdmins();
    };
    saveRow.appendChild(saveBtn);
    item.appendChild(saveRow);
    card.appendChild(item);
  }
  box.appendChild(card);
}

bootstrap();
</script>
</body>
</html>
"""


class PortalHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, code: int, obj: Any) -> None:
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, code: int, body: str) -> None:
        raw = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0 or length > 256_000:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _current_web_user(self) -> dict[str, Any] | None:
        auth = (self.headers.get("Authorization") or "").strip()
        if not auth.lower().startswith("bearer "):
            return None
        token = auth[7:].strip()
        if not token:
            return None
        thash = hash_token(token)
        ts = now_ms()
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.*
                    FROM banka_bot_web_sessions s
                    JOIN banka_bot_web_admins a ON a.web_uid = s.web_uid
                    WHERE s.token_hash = %s AND s.expires_ms > %s
                    LIMIT 1;
                    """,
                    (thash, ts),
                )
                row = cur.fetchone()
                return serialize_admin(row) if row else None

    def _require_perm(self, perm: str | None = None) -> dict[str, Any] | None:
        user = self._current_web_user()
        if not user:
            self._send_json(401, {"error": "Требуется авторизация"})
            return None
        if user["role"] not in (ROLE_SUPER_ADMIN, ROLE_MODERATOR):
            self._send_json(403, {"error": "Вы не админ и не модератор"})
            return None
        if perm and not has_perm(user, perm):
            self._send_json(403, {"error": f"Недостаточно прав ({perm})"})
            return None
        return user

    def do_GET(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"

        if path == "/health":
            self._send_json(200, {"ok": True})
            return

        if path in ("/", "/admin"):
            self._send_html(200, HTML_PAGE)
            return

        if path == "/api/auth/status":
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM banka_bot_web_admins WHERE role = %s LIMIT 1;", (ROLE_SUPER_ADMIN,))
                    has_super = cur.fetchone() is not None
            self._send_json(200, {"has_super_admin": has_super})
            return

        if path == "/api/auth/me":
            user = self._current_web_user()
            if not user:
                self._send_json(401, {"error": "Не авторизован"})
                return
            self._send_json(200, {"user": user})
            return

        if path == "/api/stats":
            if not self._require_perm("view_stats"):
                return
            now_msk = datetime.now(MSK_TZ)
            today_mid = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
            day_ms = int(today_mid.timestamp() * 1000)
            week_ms = now_ms() - 7 * 86_400_000
            month_ms = now_ms() - 30 * 86_400_000

            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COUNT(*) FILTER (WHERE status_tag = 'PAID') AS all_deals,
                            COALESCE(SUM(stars_amount) FILTER (WHERE status_tag = 'PAID'), 0) AS all_stars,
                            COUNT(*) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s) AS day_deals,
                            COALESCE(SUM(stars_amount) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s), 0) AS day_stars,
                            COUNT(*) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s) AS week_deals,
                            COALESCE(SUM(stars_amount) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s), 0) AS week_stars,
                            COUNT(*) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s) AS month_deals,
                            COALESCE(SUM(stars_amount) FILTER (WHERE status_tag = 'PAID' AND COALESCE(paid_ms, created_ms) >= %s), 0) AS month_stars,
                            COUNT(*) FILTER (WHERE status_tag = 'PENDING') AS pending_count,
                            COUNT(*) FILTER (WHERE status_tag = 'CANCELLED') AS cancelled_count,
                            COUNT(DISTINCT buyer_uid) FILTER (WHERE status_tag = 'PAID') AS paying_users
                        FROM banka_stars_orders;
                        """,
                        (day_ms, day_ms, week_ms, week_ms, month_ms, month_ms),
                    )
                    agg = cur.fetchone() or {}
                    cur.execute("SELECT COUNT(*) AS cnt FROM banka_bot_users;")
                    bot_users_cnt = int((cur.fetchone() or {}).get("cnt") or 0)
                    cur.execute(
                        """
                        SELECT item_kind,
                               COUNT(*) AS deals,
                               COALESCE(SUM(stars_amount), 0) AS stars
                        FROM banka_stars_orders
                        WHERE status_tag = 'PAID'
                        GROUP BY item_kind
                        ORDER BY stars DESC;
                        """
                    )
                    by_kind_rows = cur.fetchall()

            kind_names = {
                "PREMIUM_10": "Premium 10 дней",
                "PREMIUM_30": "Premium 30 дней",
                "COINS": "Монеты GDZgo",
                "BACKGROUND": "Фоны профиля",
            }
            self._send_json(
                200,
                {
                    "day": {"deals": int(agg.get("day_deals") or 0), "stars": int(agg.get("day_stars") or 0)},
                    "week": {"deals": int(agg.get("week_deals") or 0), "stars": int(agg.get("week_stars") or 0)},
                    "month": {"deals": int(agg.get("month_deals") or 0), "stars": int(agg.get("month_stars") or 0)},
                    "all": {"deals": int(agg.get("all_deals") or 0), "stars": int(agg.get("all_stars") or 0)},
                    "pending_count": int(agg.get("pending_count") or 0),
                    "cancelled_count": int(agg.get("cancelled_count") or 0),
                    "paying_users_count": int(agg.get("paying_users") or 0),
                    "bot_users_count": bot_users_cnt,
                    "by_kind": [
                        {
                            "item_kind": r["item_kind"],
                            "kind_label": kind_names.get(str(r["item_kind"]).upper(), str(r["item_kind"])),
                            "deals": int(r["deals"]),
                            "stars": int(r["stars"]),
                        }
                        for r in by_kind_rows
                    ],
                },
            )
            return

        if path == "/api/users":
            if not self._require_perm("view_users"):
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH ord_agg AS (
                            SELECT telegram_user_id,
                                   COALESCE(SUM(stars_amount) FILTER (WHERE status_tag = 'PAID'), 0) AS spent_stars,
                                   COUNT(*) FILTER (WHERE status_tag = 'PAID') AS paid_cnt,
                                   MAX(buyer_uid) AS last_buyer_uid,
                                   MAX(buyer_login) FILTER (WHERE buyer_login <> '') AS last_buyer_login
                            FROM banka_stars_orders
                            WHERE telegram_user_id > 0
                            GROUP BY telegram_user_id
                        )
                        SELECT bu.tg_user_id, bu.tg_username, bu.first_name, bu.last_name,
                               bu.first_seen_ms, bu.last_seen_ms,
                               COALESCE(NULLIF(bu.linked_gdz_uid, 0), oa.last_buyer_uid, 0) AS resolved_uid,
                               COALESCE(NULLIF(bu.linked_gdz_login, ''), oa.last_buyer_login, '') AS fallback_login,
                               COALESCE(oa.spent_stars, 0) AS total_stars_spent,
                               COALESCE(oa.paid_cnt, 0) AS paid_orders_count,
                               p.entry_tag AS p_login,
                               p.title_first AS p_first,
                               p.title_last AS p_last,
                               p.cohort_code AS p_class,
                               COALESCE(p.vip_flag, FALSE) AS p_vip,
                               COALESCE(p.vault_tokens, 0) AS p_coins
                        FROM banka_bot_users bu
                        LEFT JOIN ord_agg oa ON oa.telegram_user_id = bu.tg_user_id
                        LEFT JOIN banka_profiles_vault p
                               ON p.rec_uid = COALESCE(NULLIF(bu.linked_gdz_uid, 0), oa.last_buyer_uid, 0)
                        ORDER BY bu.last_seen_ms DESC
                        LIMIT 500;
                        """
                    )
                    rows = cur.fetchall()

            users_out = []
            for r in rows:
                full_name = f"{r.get('p_first') or ''} {r.get('p_last') or ''}".strip()
                users_out.append(
                    {
                        "tg_user_id": int(r["tg_user_id"]),
                        "tg_username": str(r.get("tg_username") or ""),
                        "first_name": str(r.get("first_name") or ""),
                        "last_name": str(r.get("last_name") or ""),
                        "first_seen_ms": int(r.get("first_seen_ms") or 0),
                        "last_seen_ms": int(r.get("last_seen_ms") or 0),
                        "gdz_uid": int(r.get("resolved_uid") or 0),
                        "gdz_login": str(r.get("p_login") or r.get("fallback_login") or ""),
                        "gdz_full_name": full_name,
                        "gdz_class": str(r.get("p_class") or ""),
                        "gdz_vip": bool(r.get("p_vip")),
                        "gdz_coins": int(r.get("p_coins") or 0),
                        "total_stars_spent": int(r.get("total_stars_spent") or 0),
                        "paid_orders_count": int(r.get("paid_orders_count") or 0),
                    }
                )
            self._send_json(200, {"users": users_out})
            return

        if path.startswith("/api/users/") and path.endswith("/orders"):
            if not (has_perm(self._current_web_user(), "view_users") or has_perm(self._current_web_user(), "view_orders")):
                self._require_perm("view_users")
                return
            try:
                tg_id = int(path.split("/")[3])
            except Exception:
                self._send_json(400, {"error": "Некорректный Telegram ID"})
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT linked_gdz_uid FROM banka_bot_users WHERE tg_user_id = %s;", (tg_id,))
                    bu = cur.fetchone()
                    linked_uid = int(bu["linked_gdz_uid"]) if bu else 0
                    cur.execute(
                        """
                        SELECT o.order_uid, o.pay_code, o.buyer_uid, o.buyer_login,
                               o.item_kind, o.plan_days, o.coins_amount, o.target_bg_id,
                               o.stars_amount, o.status_tag, o.telegram_user_id,
                               o.created_ms, o.paid_ms,
                               COALESCE(b.title_txt, '') AS bg_title
                        FROM banka_stars_orders o
                        LEFT JOIN banka_shop_backgrounds b ON b.bg_uid = o.target_bg_id
                        WHERE o.telegram_user_id = %s OR (%s > 0 AND o.buyer_uid = %s)
                        ORDER BY o.order_uid DESC
                        LIMIT 200;
                        """,
                        (tg_id, linked_uid, linked_uid),
                    )
                    rows = cur.fetchall()
            out = []
            for o in rows:
                summary, _ = describe_order(o)
                out.append(
                    {
                        "order_uid": int(o["order_uid"]),
                        "pay_code": str(o["pay_code"]),
                        "stars_amount": int(o["stars_amount"]),
                        "status_tag": str(o["status_tag"]),
                        "item_summary": summary,
                        "created_ms": int(o["created_ms"]),
                        "paid_ms": int(o["paid_ms"]) if o["paid_ms"] else None,
                    }
                )
            self._send_json(200, {"orders": out})
            return

        if path == "/api/orders":
            if not self._require_perm("view_orders"):
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT o.order_uid, o.pay_code, o.buyer_uid, o.buyer_login,
                               o.item_kind, o.plan_days, o.coins_amount, o.target_bg_id,
                               o.stars_amount, o.status_tag, o.telegram_user_id,
                               o.created_ms, o.paid_ms,
                               COALESCE(b.title_txt, '') AS bg_title
                        FROM banka_stars_orders o
                        LEFT JOIN banka_shop_backgrounds b ON b.bg_uid = o.target_bg_id
                        ORDER BY o.order_uid DESC
                        LIMIT 150;
                        """
                    )
                    rows = cur.fetchall()
            out = []
            for o in rows:
                summary, _ = describe_order(o)
                out.append(
                    {
                        "order_uid": int(o["order_uid"]),
                        "pay_code": str(o["pay_code"]),
                        "buyer_login": str(o.get("buyer_login") or ""),
                        "telegram_user_id": int(o.get("telegram_user_id") or 0),
                        "stars_amount": int(o["stars_amount"]),
                        "status_tag": str(o["status_tag"]),
                        "item_summary": summary,
                        "created_ms": int(o["created_ms"]),
                        "paid_ms": int(o["paid_ms"]) if o["paid_ms"] else None,
                    }
                )
            self._send_json(200, {"orders": out})
            return

        if path == "/api/messages/history":
            user = self._current_web_user()
            if not user or not (has_perm(user, "send_dm") or has_perm(user, "send_broadcast")):
                self._require_perm("send_dm")
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM banka_bot_messages_log ORDER BY log_uid DESC LIMIT 80;"
                    )
                    logs = cur.fetchall()
            self._send_json(200, {"logs": logs})
            return

        if path == "/api/admins":
            user = self._require_perm(None)
            if not user:
                return
            if user["role"] != ROLE_SUPER_ADMIN:
                self._send_json(403, {"error": "Только Супер-Администратор управляет модераторами"})
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM banka_bot_web_admins ORDER BY web_uid ASC;")
                    accounts = [serialize_admin(r) for r in cur.fetchall()]
            self._send_json(200, {"accounts": accounts})
            return

        self._send_json(404, {"error": "Не найдено"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path.rstrip("/")
        body = self._read_json()

        if path == "/api/auth/register":
            login = str(body.get("login") or "").strip().lower()
            password = str(body.get("password") or "")
            display_name = str(body.get("display_name") or "").strip()[:120] or login
            if len(login) < 3 or len(login) > 64:
                self._send_json(400, {"error": "Логин должен быть от 3 до 64 символов"})
                return
            if len(password) < 6:
                self._send_json(400, {"error": "Пароль должен содержать минимум 6 символов"})
                return
            salt_hex, pwd_hash = hash_password(password)
            ts = now_ms()
            raw_token = secrets.token_urlsafe(32)
            thash = hash_token(raw_token)
            try:
                with db_connect() as conn:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute("LOCK TABLE banka_bot_web_admins IN EXCLUSIVE MODE;")
                            cur.execute("SELECT COUNT(*) AS cnt FROM banka_bot_web_admins;")
                            cnt = int((cur.fetchone() or {}).get("cnt") or 0)
                            is_first = cnt == 0
                            role = ROLE_SUPER_ADMIN if is_first else ROLE_USER
                            perms_json = json.dumps({k: True for k in ALL_PERMISSIONS}) if is_first else "{}"
                            cur.execute(
                                """
                                INSERT INTO banka_bot_web_admins (
                                    login, display_name, pwd_salt, pwd_hash, role, perms_json, created_ms, last_login_ms
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING *;
                                """,
                                (login, display_name, salt_hex, pwd_hash, role, perms_json, ts, ts),
                            )
                            new_user = cur.fetchone()
                            cur.execute(
                                """
                                INSERT INTO banka_bot_web_sessions (token_hash, web_uid, created_ms, expires_ms)
                                VALUES (%s, %s, %s, %s);
                                """,
                                (thash, new_user["web_uid"], ts, ts + 7 * 86_400_000),
                            )
                self._send_json(200, {"token": raw_token, "user": serialize_admin(new_user)})
            except psycopg.errors.UniqueViolation:
                self._send_json(409, {"error": "Такой логин уже занят"})
            except Exception as e:
                self._send_json(500, {"error": f"Ошибка регистрации: {e}"})
            return

        if path == "/api/auth/login":
            login = str(body.get("login") or "").strip().lower()
            password = str(body.get("password") or "")
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM banka_bot_web_admins WHERE login = %s LIMIT 1;", (login,))
                    row = cur.fetchone()
                    if not row or not verify_password(password, row["pwd_salt"], row["pwd_hash"]):
                        self._send_json(401, {"error": "Неверный логин или пароль"})
                        return
                    ts = now_ms()
                    raw_token = secrets.token_urlsafe(32)
                    thash = hash_token(raw_token)
                    cur.execute(
                        "UPDATE banka_bot_web_admins SET last_login_ms = %s WHERE web_uid = %s;",
                        (ts, row["web_uid"]),
                    )
                    cur.execute(
                        """
                        INSERT INTO banka_bot_web_sessions (token_hash, web_uid, created_ms, expires_ms)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (thash, row["web_uid"], ts, ts + 7 * 86_400_000),
                    )
            self._send_json(200, {"token": raw_token, "user": serialize_admin(row)})
            return

        if path == "/api/auth/logout":
            auth = (self.headers.get("Authorization") or "").strip()
            if auth.lower().startswith("bearer "):
                thash = hash_token(auth[7:].strip())
                with db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM banka_bot_web_sessions WHERE token_hash = %s;", (thash,))
            self._send_json(200, {"ok": True})
            return

        if path == "/api/orders/cancel":
            if not self._require_perm("manage_orders"):
                return
            try:
                order_uid = int(body.get("order_uid") or 0)
            except Exception:
                order_uid = 0
            if order_uid <= 0:
                self._send_json(400, {"error": "Некорректный ID заказа"})
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE banka_stars_orders SET status_tag = 'CANCELLED' WHERE order_uid = %s AND status_tag = 'PENDING';",
                        (order_uid,),
                    )
            delete_uncleaned_invoices_for_order(order_uid)
            self._send_json(200, {"ok": True})
            return

        if path == "/api/messages/dm":
            user = self._require_perm("send_dm")
            if not user:
                return
            try:
                tg_id = int(body.get("tg_user_id") or 0)
            except Exception:
                tg_id = 0
            text = str(body.get("text") or "").strip()
            if tg_id == 0 or not text:
                self._send_json(400, {"error": "Укажите Telegram ID и текст сообщения"})
                return
            try:
                bot.send_message(tg_id, text[:4000])
            except Exception as e:
                self._send_json(400, {"error": f"Не удалось доставить в Telegram: {e}"})
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO banka_bot_messages_log (
                            sender_web_uid, sender_login, kind, target_tg_user_id,
                            message_text, sent_count, failed_count, created_ms
                        )
                        VALUES (%s, %s, 'DM', %s, %s, 1, 0, %s);
                        """,
                        (user["web_uid"], user["login"], tg_id, text[:4000], now_ms()),
                    )
            self._send_json(200, {"message": f"Сообщение отправлено пользователю {tg_id}."})
            return

        if path == "/api/messages/broadcast":
            user = self._require_perm("send_broadcast")
            if not user:
                return
            text = str(body.get("text") or "").strip()
            if not text:
                self._send_json(400, {"error": "Введите текст рассылки"})
                return
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT tg_user_id FROM banka_bot_users ORDER BY last_seen_ms DESC;")
                    targets = [int(r["tg_user_id"]) for r in cur.fetchall()]
            sent = 0
            failed = 0
            for tid in targets:
                try:
                    bot.send_message(tid, text[:4000])
                    sent += 1
                except Exception:
                    failed += 1
                time.sleep(0.05)
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO banka_bot_messages_log (
                            sender_web_uid, sender_login, kind, target_tg_user_id,
                            message_text, sent_count, failed_count, created_ms
                        )
                        VALUES (%s, %s, 'BROADCAST', 0, %s, %s, %s, %s);
                        """,
                        (user["web_uid"], user["login"], text[:4000], sent, failed, now_ms()),
                    )
            self._send_json(200, {"sent": sent, "failed": failed})
            return

        if path == "/api/admins/role":
            user = self._require_perm(None)
            if not user:
                return
            if user["role"] != ROLE_SUPER_ADMIN:
                self._send_json(403, {"error": "Только Супер-Администратор может менять роли"})
                return
            try:
                target_uid = int(body.get("web_uid") or 0)
            except Exception:
                target_uid = 0
            new_role = str(body.get("role") or ROLE_USER).upper()
            if new_role not in (ROLE_USER, ROLE_MODERATOR):
                self._send_json(400, {"error": "Допустимы только роли USER и MODERATOR"})
                return
            if target_uid == user["web_uid"]:
                self._send_json(400, {"error": "Нельзя изменить собственную роль Супер-Администратора"})
                return
            perms = safe_json_dict(body.get("permissions")) if new_role == ROLE_MODERATOR else {}
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT role FROM banka_bot_web_admins WHERE web_uid = %s;", (target_uid,))
                    target = cur.fetchone()
                    if not target:
                        self._send_json(404, {"error": "Аккаунт не найден"})
                        return
                    if target["role"] == ROLE_SUPER_ADMIN:
                        self._send_json(400, {"error": "Нельзя менять роль Супер-Администратора"})
                        return
                    cur.execute(
                        "UPDATE banka_bot_web_admins SET role = %s, perms_json = %s WHERE web_uid = %s;",
                        (new_role, json.dumps(perms), target_uid),
                    )
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"error": "Не найдено"})


def start_web_server() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), PortalHandler)
    server.serve_forever()


if __name__ == "__main__":
    init_bot_tables()
    threading.Thread(target=cancelled_invoice_watcher, daemon=True).start()
    threading.Thread(target=start_web_server, daemon=True).start()
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=25, skip_pending=False)
        except Exception as err:
            print(f"Polling error: {err}")
            time.sleep(3)
