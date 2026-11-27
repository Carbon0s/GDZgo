import asyncio
import logging
import math
import os
import secrets
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, Update
from telegram.error import TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("gdzgo_stars_bot")

# Для Render: задайте BOT_TOKEN и DATABASE_URL в Environment Variables (убрав значения по умолчанию перед публикацией кода)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "9054158642:AAGYGFuhPci5fnWqMg2Q6lAVk9t7MbsmgIM").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://avnadmin:AVNS_A094KJpWYOSX9t3_eM6@youme-krossmag.l.aivencloud.com:25520/defaultdb?sslmode=require").strip()

TBL_USERS = "banka_profiles_vault"
TBL_CONFIG = "banka_system_config"
TBL_NOTICES = "banka_user_notices"
TBL_SHOP_BG = "banka_shop_backgrounds"
TBL_USER_BG = "banka_user_backgrounds"
TBL_STARS_ORDERS = "banka_stars_orders"
TBL_STARS_INVOICES = "banka_stars_invoices"


def now_ms() -> int:
    return int(time.time() * 1000)


@contextmanager
def get_conn():
    dsn = DATABASE_URL
    if "sslmode=" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}sslmode=require"
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_tables():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TBL_STARS_ORDERS} (
                order_uid SERIAL PRIMARY KEY,
                pay_code VARCHAR(16) UNIQUE NOT NULL,
                buyer_uid INT NOT NULL,
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
                created_ms BIGINT NOT NULL,
                paid_ms BIGINT DEFAULT NULL,
                created_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {TBL_STARS_INVOICES} (
                inv_uid SERIAL PRIMARY KEY,
                order_uid INT NOT NULL,
                pay_code VARCHAR(16) DEFAULT '',
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                cleaned_up BOOLEAN DEFAULT FALSE,
                created_ms BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_{TBL_STARS_INVOICES}_active
                ON {TBL_STARS_INVOICES} (cleaned_up, order_uid);
            """
        )


def generate_pay_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))


def get_system_config(cur) -> dict:
    cur.execute(
        f"""
        SELECT COALESCE(stars_10d_price, 50) AS stars_10d_price,
               COALESCE(stars_30d_price, 100) AS stars_30d_price,
               COALESCE(coins_per_star, 5.0) AS coins_per_star,
               COALESCE(stars_bot_username, '') AS stars_bot_username
        FROM {TBL_CONFIG}
        WHERE cfg_uid = 1;
        """
    )
    row = cur.fetchone()
    if not row:
        return {
            "stars_10d_price": 50,
            "stars_30d_price": 100,
            "coins_per_star": 5.0,
            "stars_bot_username": "",
        }
    return dict(row)


def find_user_by_handle(cur, raw_query: str):
    clean = raw_query.strip().lstrip("@").lower()
    if not clean:
        return None
    cur.execute(
        f"""
        SELECT rec_uid, entry_tag, title_first, title_last, chat_username, vip_flag, vault_tokens
        FROM {TBL_USERS}
        WHERE LOWER(entry_tag) = %s
           OR LOWER(COALESCE(chat_username, '')) = %s
        LIMIT 1;
        """,
        (clean, clean),
    )
    return cur.fetchone()


def format_order_title(cur, order: dict) -> tuple[str, str]:
    kind = (order.get("item_kind") or "").upper()
    if kind == "BACKGROUND":
        cur.execute(
            f"SELECT title_txt FROM {TBL_SHOP_BG} WHERE bg_uid = %s;",
            (order.get("target_bg_id") or 0,),
        )
        bg = cur.fetchone()
        bg_title = bg["title_txt"] if bg else f"#{order.get('target_bg_id')}"
        return (
            f"Фон профиля «{bg_title}»",
            f"Покупка фона «{bg_title}» для аккаунта {order.get('buyer_login') or order['buyer_uid']}",
        )
    if kind == "COINS":
        coins = int(order.get("coins_amount") or 0)
        return (
            f"{coins} монет GDZgo",
            f"Пополнение баланса на {coins} монет для {order.get('buyer_login') or order['buyer_uid']}",
        )
    days = int(order.get("plan_days") or 30)
    return (
        f"GDZgo Premium ({days} дн.)",
        f"Активация Premium на {days} дн. для {order.get('buyer_login') or order['buyer_uid']}",
    )


def check_order_still_payable(cur, order: dict) -> tuple[bool, str]:
    if not order:
        return False, "Заказ не найден."
    if order["status_tag"] != "PENDING":
        return False, "Заказ отменён или уже закрыт."
    if (order.get("item_kind") or "").upper() == "BACKGROUND":
        bg_id = int(order.get("target_bg_id") or 0)
        cur_ms = now_ms()
        cur.execute(
            f"""
            SELECT is_active, max_qty, sold_count, expires_ms, publish_at_ms
            FROM {TBL_SHOP_BG}
            WHERE bg_uid = %s;
            """,
            (bg_id,),
        )
        bg = cur.fetchone()
        if not bg or not bg["is_active"]:
            return False, "Фон снят с продажи — счёт отменён."
        if bg["publish_at_ms"] and cur_ms < int(bg["publish_at_ms"]):
            return False, "Фон ещё не вышел в продажу."
        if bg["expires_ms"] and cur_ms >= int(bg["expires_ms"]):
            return False, "Срок продажи лимитного фона истёк."
        if bg["max_qty"] and int(bg["sold_count"] or 0) >= int(bg["max_qty"]):
            return False, "Тираж фона уже раскуплен."
        cur.execute(
            f"SELECT 1 FROM {TBL_USER_BG} WHERE owner_uid = %s AND bg_uid = %s LIMIT 1;",
            (int(order["buyer_uid"]), bg_id),
        )
        if cur.fetchone():
            return False, "Этот фон уже есть в коллекции аккаунта."
    return True, ""


def pop_active_invoices_for_order_sync(order_uid: int) -> list[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TBL_STARS_INVOICES}
            SET cleaned_up = TRUE
            WHERE order_uid = %s AND cleaned_up = FALSE
            RETURNING chat_id, message_id, pay_code;
            """,
            (order_uid,),
        )
        return [dict(r) for r in cur.fetchall()]


def save_sent_invoice_sync(order_uid: int, pay_code: str, chat_id: int, message_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {TBL_STARS_INVOICES} (order_uid, pay_code, chat_id, message_id, cleaned_up, created_ms)
            VALUES (%s, %s, %s, %s, FALSE, %s);
            """,
            (order_uid, pay_code, chat_id, message_id, now_ms()),
        )


def cancel_order_by_id_sync(order_uid: int) -> dict | None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TBL_STARS_ORDERS}
            SET status_tag = 'CANCELLED'
            WHERE order_uid = %s AND status_tag = 'PENDING'
            RETURNING *;
            """,
            (order_uid,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


async def cleanup_invoice_messages(bot, invoices: list[dict], notify_text: str | None = None):
    seen_chats = set()
    for inv in invoices:
        chat_id = int(inv["chat_id"])
        msg_id = int(inv["message_id"])
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except TelegramError as e:
            logger.debug("Could not delete invoice msg %s/%s: %s", chat_id, msg_id, e)
        if notify_text and chat_id not in seen_chats:
            seen_chats.add(chat_id)
            try:
                await bot.send_message(chat_id=chat_id, text=notify_text, parse_mode="HTML")
            except TelegramError:
                pass


async def send_stars_invoice_for_order(
    bot,
    chat_id: int,
    order: dict,
    title: str,
    description: str,
):
    order_uid = int(order["order_uid"])
    # Удаляем предыдущий дубликат счёта для этого же заказа, если он висел в чате
    old_invs = await asyncio.to_thread(pop_active_invoices_for_order_sync, order_uid)
    if old_invs:
        await cleanup_invoice_messages(bot, old_invs, notify_text=None)

    stars = max(1, int(order["stars_amount"]))
    payload = f"gdz_order:{order_uid}:{order['pay_code']}"
    # В sendInvoice первая кнопка обязательно должна иметь pay=True
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"⭐ Оплатить {stars} Stars", pay=True)],
            [InlineKeyboardButton("❌ Отменить счёт", callback_data=f"cancel_inv:{order_uid}")],
        ]
    )
    sent = await bot.send_invoice(
        chat_id=chat_id,
        title=title[:32],
        description=description[:255],
        payload=payload,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=title[:32], amount=stars)],
        reply_markup=kb,
    )
    await asyncio.to_thread(
        save_sent_invoice_sync,
        order_uid,
        order["pay_code"],
        sent.chat_id,
        sent.message_id,
    )


def fulfill_paid_order_in_db(order_uid: int, tg_user_id: int, tg_charge_id: str) -> tuple[str, dict | None]:
    """
    Возвращает ("OK", order), ("ALREADY_PAID", order) или ("NEED_REFUND", order),
    если заказ был отменён до зачисления.
    """
    cur_ms = now_ms()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {TBL_STARS_ORDERS} WHERE order_uid = %s FOR UPDATE;",
            (order_uid,),
        )
        order = cur.fetchone()
        if not order:
            return "NEED_REFUND", None

        if order["status_tag"] == "PAID":
            return "ALREADY_PAID", dict(order)

        if order["status_tag"] != "PENDING":
            return "NEED_REFUND", dict(order)

        payable, _ = check_order_still_payable(cur, dict(order))
        if not payable:
            cur.execute(
                f"UPDATE {TBL_STARS_ORDERS} SET status_tag = 'CANCELLED' WHERE order_uid = %s;",
                (order_uid,),
            )
            return "NEED_REFUND", dict(order)

        if tg_charge_id:
            cur.execute(
                f"SELECT order_uid FROM {TBL_STARS_ORDERS} WHERE telegram_charge_id = %s LIMIT 1;",
                (tg_charge_id,),
            )
            if cur.fetchone():
                return "ALREADY_PAID", dict(order)

        buyer_uid = int(order["buyer_uid"])
        kind = (order["item_kind"] or "").upper()
        stars = int(order["stars_amount"])

        if kind in ("PREMIUM_10", "PREMIUM_30", "PREMIUM"):
            days = max(1, int(order["plan_days"] or (10 if kind == "PREMIUM_10" else 30)))
            cur.execute(
                f"SELECT vip_flag, premium_until_ms FROM {TBL_USERS} WHERE rec_uid = %s FOR UPDATE;",
                (buyer_uid,),
            )
            u = cur.fetchone()
            if u:
                is_perm = bool(u["vip_flag"]) and u["premium_until_ms"] is None
                if is_perm:
                    new_until = None
                else:
                    old_until = u["premium_until_ms"]
                    base = old_until if (u["vip_flag"] and old_until and old_until > cur_ms) else cur_ms
                    new_until = base + days * 86_400_000
                cur.execute(
                    f"UPDATE {TBL_USERS} SET vip_flag = TRUE, premium_until_ms = %s WHERE rec_uid = %s;",
                    (new_until, buyer_uid),
                )
                cur.execute(
                    f"""
                    INSERT INTO {TBL_NOTICES} (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label)
                    VALUES (%s, 'PREMIUM_GRANTED', %s, %s, %s, 'Telegram Stars');
                    """,
                    (
                        buyer_uid,
                        f"Оплачен Premium на {days} дн.",
                        f"Оплата ⭐ {stars} успешно подтверждена! Подписка Premium активирована на {days} дн.",
                        f"Код {order['pay_code']}",
                    ),
                )

        elif kind == "COINS":
            coins = max(1, int(order["coins_amount"] or 0))
            cur.execute(
                f"UPDATE {TBL_USERS} SET vault_tokens = COALESCE(vault_tokens, 0) + %s WHERE rec_uid = %s;",
                (coins, buyer_uid),
            )
            cur.execute(
                f"""
                INSERT INTO {TBL_NOTICES} (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label)
                VALUES (%s, 'COIN_PURCHASE_REWARD', %s, %s, %s, 'Telegram Stars');
                """,
                (
                    buyer_uid,
                    f"Начислено {coins} монет",
                    f"Оплата ⭐ {stars} подтверждена. На ваш баланс зачислено {coins} монет.",
                    f"Код {order['pay_code']}",
                ),
            )

        elif kind == "BACKGROUND":
            bg_id = int(order["target_bg_id"] or 0)
            cur.execute(
                f"SELECT title_txt FROM {TBL_SHOP_BG} WHERE bg_uid = %s FOR UPDATE;",
                (bg_id,),
            )
            bg = cur.fetchone()
            bg_title = bg["title_txt"] if bg else f"#{bg_id}"
            cur.execute(
                f"""
                INSERT INTO {TBL_USER_BG} (owner_uid, bg_uid, granted_via)
                VALUES (%s, %s, 'STARS')
                ON CONFLICT (owner_uid, bg_uid) DO NOTHING
                RETURNING bg_uid;
                """,
                (buyer_uid, bg_id),
            )
            if cur.fetchone():
                cur.execute(
                    f"""
                    UPDATE {TBL_SHOP_BG}
                    SET sold_count = COALESCE(sold_count, 0) + 1,
                        earned_stars = COALESCE(earned_stars, 0) + %s
                    WHERE bg_uid = %s;
                    """,
                    (stars, bg_id),
                )
            cur.execute(
                f"""
                INSERT INTO {TBL_NOTICES} (target_uid, kind_tag, title_txt, body_txt, reason_txt, issuer_label)
                VALUES (%s, 'COIN_PURCHASE_REWARD', %s, %s, %s, 'Telegram Stars');
                """,
                (
                    buyer_uid,
                    f"Куплен фон «{bg_title}»",
                    f"Оплата ⭐ {stars} прошла успешно! Фон «{bg_title}» добавлен в вашу коллекцию.",
                    f"Код {order['pay_code']}",
                ),
            )

        cur.execute(
            f"""
            UPDATE {TBL_STARS_ORDERS}
            SET status_tag = 'PAID',
                telegram_user_id = %s,
                telegram_charge_id = %s,
                paid_ms = %s,
                ui_celebrated = FALSE
            WHERE order_uid = %s
            RETURNING *;
            """,
            (tg_user_id, tg_charge_id, cur_ms, order_uid),
        )
        updated_order = dict(cur.fetchone())
        cur.execute(
            f"UPDATE {TBL_STARS_INVOICES} SET cleaned_up = TRUE WHERE order_uid = %s;",
            (order_uid,),
        )
        return "OK", updated_order


def poll_cancelled_invoices_sync() -> list[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT inv.inv_uid, inv.order_uid, inv.pay_code, inv.chat_id, inv.message_id, o.status_tag
            FROM {TBL_STARS_INVOICES} inv
            JOIN {TBL_STARS_ORDERS} o ON o.order_uid = inv.order_uid
            WHERE inv.cleaned_up = FALSE
              AND o.status_tag <> 'PENDING'
            ORDER BY inv.inv_uid ASC
            LIMIT 50;
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        if rows:
            ids = [r["inv_uid"] for r in rows]
            cur.execute(
                f"UPDATE {TBL_STARS_INVOICES} SET cleaned_up = TRUE WHERE inv_uid = ANY(%s);",
                (ids,),
            )
        return rows


async def cancelled_invoice_watcher(bot):
    while True:
        try:
            stale = await asyncio.to_thread(poll_cancelled_invoices_sync)
            by_order: dict[int, list[dict]] = {}
            for r in stale:
                by_order.setdefault(int(r["order_uid"]), []).append(r)

            for _, invs in by_order.items():
                status = (invs[0].get("status_tag") or "").upper()
                code = invs[0].get("pay_code") or ""
                msg = (
                    f"❌ Счёт <code>{code}</code> отменён (оплата отменена в приложении). Звёзды не списаны."
                    if status == "CANCELLED"
                    else None
                )
                await cleanup_invoice_messages(bot, invs, notify_text=msg)
        except Exception as e:
            logger.warning("Watcher error: %s", e)
        await asyncio.sleep(2.0)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔑 Ввести код из приложения", callback_data="ask_code")],
            [InlineKeyboardButton("🛍 Купить в боте по @логину", callback_data="ask_username")],
        ]
    )


async def handle_code_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_code: str):
    code = raw_code.strip().upper()
    chat_id = update.effective_chat.id

    def _lookup():
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {TBL_STARS_ORDERS} WHERE UPPER(pay_code) = %s LIMIT 1;",
                (code,),
            )
            order = cur.fetchone()
            if not order:
                return None, False, "NOT_FOUND", "", ""
            o = dict(order)
            if o["status_tag"] == "PAID":
                return o, False, "PAID", "", ""
            if o["status_tag"] != "PENDING":
                return o, False, "CANCELLED", "", ""
            payable, reason = check_order_still_payable(cur, o)
            if not payable:
                cur.execute(
                    f"UPDATE {TBL_STARS_ORDERS} SET status_tag = 'CANCELLED' WHERE order_uid = %s;",
                    (o["order_uid"],),
                )
                return o, False, reason, "", ""
            title, desc = format_order_title(cur, o)
            return o, True, "", title, desc

    order, ok, err, title, desc = await asyncio.to_thread(_lookup)
    if not order:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Заказ с кодом <code>{code}</code> не найден. Проверьте код в приложении GDZgo.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return
    if not ok:
        if err == "PAID":
            txt = f"✅ Заказ <b>{code}</b> уже оплачен! Откройте приложение GDZgo."
        elif err == "CANCELLED":
            txt = f"❌ Заказ <b>{code}</b> отменён. Оплата заблокирована — создайте новый заказ в приложении."
        else:
            txt = f"⚠️ Счёт <b>{code}</b> отменён: {err}"
        await context.bot.send_message(
            chat_id=chat_id,
            text=txt,
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return

    await send_stars_invoice_for_order(context.bot, chat_id, order, title, desc)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    args = context.args or []
    if args and args[0].lower().startswith("pay_"):
        pay_code = args[0][4:].strip().upper()
        if pay_code:
            await handle_code_lookup(update, context, pay_code)
            return

    await update.message.reply_text(
        "⭐ <b>Оплата GDZgo через Telegram Stars</b>\n\n"
        "• Отправьте <b>код оплаты</b> из приложения GDZgo\n"
        "• Или купите Premium, монеты или фон напрямую по вашему <b>@логину</b> в GDZgo.",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data == "ask_code":
        context.user_data["awaiting"] = "code"
        await query.message.reply_text("Введите 6-значный код оплаты из приложения GDZgo:")
        return

    if data == "ask_username":
        context.user_data["awaiting"] = "username"
        await query.message.reply_text("Введите ваш @юзернейм или логин из приложения GDZgo:")
        return

    if data.startswith("cancel_inv:"):
        order_uid = int(data.split(":")[1])
        cancelled = await asyncio.to_thread(cancel_order_by_id_sync, order_uid)
        invs = await asyncio.to_thread(pop_active_invoices_for_order_sync, order_uid)
        if not invs and query.message:
            invs = [{"chat_id": query.message.chat_id, "message_id": query.message.message_id, "pay_code": ""}]
        code_txt = f" <code>{cancelled['pay_code']}</code>" if cancelled else ""
        await cleanup_invoice_messages(
            context.bot,
            invs,
            notify_text=f"❌ Счёт{code_txt} отменён. Звёзды не списаны.",
        )
        return

    if data.startswith("buy:"):
        target_uid = int(context.user_data.get("target_uid") or 0)
        target_login = context.user_data.get("target_login") or ""
        if target_uid <= 0:
            await query.message.reply_text("Сначала выберите аккаунт через /start.", reply_markup=main_menu_keyboard())
            return

        parts = data.split(":")
        item_tag = parts[1]
        cur_ms = now_ms()

        def _create_direct_order():
            with get_conn() as conn, conn.cursor() as cur:
                cfg = get_system_config(cur)
                item_kind = "PREMIUM_30"
                plan_days = 0
                coins_amount = 0
                target_bg_id = 0
                stars_amount = 0

                if item_tag == "prem10":
                    item_kind = "PREMIUM_10"
                    plan_days = 10
                    stars_amount = max(1, int(cfg["stars_10d_price"]))
                elif item_tag == "prem30":
                    item_kind = "PREMIUM_30"
                    plan_days = 30
                    stars_amount = max(1, int(cfg["stars_30d_price"]))
                elif item_tag == "coins":
                    stars_amount = max(1, int(parts[2]))
                    item_kind = "COINS"
                    coins_amount = max(1, round(stars_amount * float(cfg["coins_per_star"])))
                elif item_tag == "bg":
                    bg_id = int(parts[2])
                    cur.execute(
                        f"""
                        SELECT bg_uid, title_txt, price_stars, discount_pct, max_qty, sold_count, expires_ms, publish_at_ms, is_active
                        FROM {TBL_SHOP_BG}
                        WHERE bg_uid = %s;
                        """,
                        (bg_id,),
                    )
                    bg = cur.fetchone()
                    if not bg or not bg["is_active"]:
                        return None, "❌ Этот фон больше недоступен.", None, None
                    if bg["publish_at_ms"] and cur_ms < int(bg["publish_at_ms"]):
                        return None, "⏳ Этот фон ещё не поступил в продажу.", None, None
                    if bg["expires_ms"] and cur_ms >= int(bg["expires_ms"]):
                        return None, "⌛ Срок продажи этого лимитного фона истёк.", None, None
                    if bg["max_qty"] and int(bg["sold_count"] or 0) >= int(bg["max_qty"]):
                        return None, "😔 Тираж этого фона уже раскуплен.", None, None
                    base_stars = int(bg["price_stars"] or 0)
                    if base_stars <= 0:
                        return None, "❌ Для этого фона не задана цена в ⭐.", None, None
                    disc = max(0, min(99, int(bg["discount_pct"] or 0)))
                    stars_amount = max(1, math.ceil(base_stars * (100 - disc) / 100.0)) if disc > 0 else base_stars
                    item_kind = "BACKGROUND"
                    target_bg_id = bg_id
                elif item_tag == "bglist":
                    cur.execute(
                        f"""
                        SELECT bg_uid, title_txt, price_stars, discount_pct
                        FROM {TBL_SHOP_BG}
                        WHERE is_active = TRUE
                          AND COALESCE(price_stars, 0) > 0
                          AND (publish_at_ms IS NULL OR publish_at_ms <= %s)
                          AND (expires_ms IS NULL OR expires_ms > %s)
                          AND (max_qty IS NULL OR max_qty <= 0 OR COALESCE(sold_count, 0) < max_qty)
                        ORDER BY bg_uid DESC
                        LIMIT 15;
                        """,
                        (cur_ms, cur_ms),
                    )
                    return "BGLIST", [dict(x) for x in cur.fetchall()], None, None

                # Отменяем старые PENDING-заказы этого пользователя (watcher удалит старые счета в чате)
                cur.execute(
                    f"UPDATE {TBL_STARS_ORDERS} SET status_tag = 'CANCELLED' WHERE buyer_uid = %s AND status_tag = 'PENDING';",
                    (target_uid,),
                )
                pay_code = generate_pay_code()
                cur.execute(
                    f"""
                    INSERT INTO {TBL_STARS_ORDERS} (
                        pay_code, buyer_uid, buyer_login, item_kind,
                        plan_days, coins_amount, target_bg_id, stars_amount,
                        status_tag, ui_celebrated, created_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', FALSE, %s)
                    RETURNING *;
                    """,
                    (pay_code, target_uid, target_login, item_kind, plan_days, coins_amount, target_bg_id, stars_amount, cur_ms),
                )
                order = dict(cur.fetchone())
                title, desc = format_order_title(cur, order)
                return order, None, title, desc

        res, extra, title, desc = await asyncio.to_thread(_create_direct_order)
        if res == "BGLIST":
            bgs = extra
            if not bgs:
                await query.message.reply_text("Сейчас нет фонов с оплатой за ⭐.")
                return
            rows = []
            for b in bgs:
                disc = max(0, min(99, int(b["discount_pct"] or 0)))
                eff = max(1, math.ceil(int(b["price_stars"]) * (100 - disc) / 100.0)) if disc > 0 else int(b["price_stars"])
                rows.append([InlineKeyboardButton(f"🖼 {b['title_txt']} — ⭐ {eff}", callback_data=f"buy:bg:{b['bg_uid']}")])
            await query.message.reply_text("Выберите фон для покупки:", reply_markup=InlineKeyboardMarkup(rows))
            return

        if not res:
            await query.message.reply_text(extra)
            return

        await send_stars_invoice_for_order(context.bot, query.message.chat_id, res, title, desc)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    if not txt:
        return
    mode = context.user_data.get("awaiting")

    if mode == "username" or (mode is None and txt.startswith("@")):
        def _find():
            with get_conn() as conn, conn.cursor() as cur:
                return find_user_by_handle(cur, txt), get_system_config(cur)

        u, cfg = await asyncio.to_thread(_find)
        if not u:
            await update.message.reply_text(
                "❌ Пользователь с таким @логином не найден в GDZgo. Попробуйте снова или введите код оплаты:",
                reply_markup=main_menu_keyboard(),
            )
            return
        context.user_data["awaiting"] = None
        context.user_data["target_uid"] = int(u["rec_uid"])
        context.user_data["target_login"] = u["entry_tag"]
        full_name = f"{u.get('title_first') or ''} {u.get('title_last') or ''}".strip() or u["entry_tag"]
        rate = float(cfg["coins_per_star"])
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"👑 Premium 10 дн. — ⭐ {cfg['stars_10d_price']}", callback_data="buy:prem10")],
                [InlineKeyboardButton(f"👑 Premium 30 дн. — ⭐ {cfg['stars_30d_price']}", callback_data="buy:prem30")],
                [
                    InlineKeyboardButton(f"🪙 {round(25 * rate)} монет — ⭐ 25", callback_data="buy:coins:25"),
                    InlineKeyboardButton(f"🪙 {round(100 * rate)} монет — ⭐ 100", callback_data="buy:coins:100"),
                ],
                [InlineKeyboardButton("🖼 Купить фон профиля", callback_data="buy:bglist")],
            ]
        )
        await update.message.reply_text(
            f"👤 Аккаунт: <b>{full_name}</b> (<code>@{u['entry_tag']}</code>)\nВыберите товар для оплаты ⭐:",
            parse_mode="HTML",
            reply_markup=kb,
        )
        return

    clean_code = txt.upper()
    if 4 <= len(clean_code) <= 12 and clean_code.isalnum():
        context.user_data["awaiting"] = None
        await handle_code_lookup(update, context, clean_code)
    else:
        await update.message.reply_text(
            "Отправьте 6-значный код оплаты или нажмите кнопку ниже:",
            reply_markup=main_menu_keyboard(),
        )


async def on_pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    payload = query.invoice_payload or ""
    if not payload.startswith("gdz_order:"):
        await query.answer(ok=False, error_message="Некорректный платёж GDZgo.")
        return

    try:
        order_uid = int(payload.split(":")[1])

        def _check():
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {TBL_STARS_ORDERS} WHERE order_uid = %s;",
                    (order_uid,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "Заказ не найден."
                return check_order_still_payable(cur, dict(row))

        payable, reason = await asyncio.to_thread(_check)
        if not payable:
            await query.answer(ok=False, error_message=f"{reason} Звёзды не списаны.")
            invs = await asyncio.to_thread(pop_active_invoices_for_order_sync, order_uid)
            if invs:
                await cleanup_invoice_messages(
                    context.bot,
                    invs,
                    notify_text=f"❌ Оплата отклонена до списания: {reason}",
                )
            return

        await query.answer(ok=True)
    except Exception:
        logger.exception("PreCheckout check failed")
        await query.answer(ok=False, error_message="Временная ошибка проверки заказа.")


async def on_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    payload = sp.invoice_payload or ""
    order_uid = int(payload.split(":")[1])
    tg_uid = update.effective_user.id if update.effective_user else 0
    charge_id = sp.telegram_payment_charge_id or ""

    status, order = await asyncio.to_thread(fulfill_paid_order_in_db, order_uid, tg_uid, charge_id)
    invs = await asyncio.to_thread(pop_active_invoices_for_order_sync, order_uid)
    if invs:
        await cleanup_invoice_messages(context.bot, invs, notify_text=None)

    if status == "NEED_REFUND":
        refunded = False
        if tg_uid > 0 and charge_id:
            try:
                await context.bot.refund_star_payment(
                    user_id=tg_uid,
                    telegram_payment_charge_id=charge_id,
                )
                refunded = True
            except Exception:
                logger.exception("Auto-refund failed for order %s / charge %s", order_uid, charge_id)
        text = (
            "❌ Заказ уже был отменён перед оплатой — звёзды автоматически возвращены на ваш баланс Telegram."
            if refunded
            else "⚠️ Заказ был отменён. Обратитесь в поддержку GDZgo с номером платежа для возврата."
        )
        await update.message.reply_text(text)
        return

    if not order:
        await update.message.reply_text("⚠️ Платёж получен, но заказ не найден. Обратитесь в поддержку GDZgo.")
        return

    await update.message.reply_text(
        f"✅ Оплата ⭐ <b>{order['stars_amount']}</b> прошла успешно!\n"
        f"Код: <code>{order['pay_code']}</code>\n"
        "Покупка уже начислена и автоматически отобразится в приложении GDZgo.",
        parse_mode="HTML",
    )


def start_health_server_if_needed():
    port_raw = os.environ.get("PORT", "").strip()
    if not port_raw:
        return

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            return

    server = HTTPServer(("0.0.0.0", int(port_raw)), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


async def post_init(application):
    application.create_task(cancelled_invoice_watcher(application.bot))


def main():
    ensure_tables()
    start_health_server_if_needed()

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(PreCheckoutQueryHandler(on_pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_successful_payment))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logger.info("GDZgo Stars bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
