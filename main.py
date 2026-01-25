import asyncio
import json
import logging
import os
import re
import sqlite3
import tempfile
import urllib.parse
import urllib.request
from html import escape
from datetime import datetime, timedelta, timezone
from functools import partial
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from dataclasses import dataclass
from typing import Optional

from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (InlineKeyboardButton, InlineKeyboardMarkup,
                           KeyboardButton, ReplyKeyboardMarkup)
from aiogram.utils.media_group import MediaGroupBuilder

import db

ADMIN_LIST = {960217500, 8359092913, 5950335991, 45152058, 7746040125}
GROUP_LIST = {-1003580758940,}
REPORT_LIST = {960217500,}


def get_tashkent_tz() -> timezone:
    try:
        return ZoneInfo("Asia/Tashkent")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=5))


TASHKENT_TZ = get_tashkent_tz()

INFO_TEXT = """ℹ️ Bizning botda mahsulotlar haqida ma'lumot olishingiz mumkin.
⚖️ Mahsulotlar narxi kilogramm bo'yicha ko'rsatiladi.
"""
CONTACT_TEXT = """Axborot hamkorlik masalalari uchun:
☎️ Telefon: +998955309999
☎️ Telefon: +998953309999
📍 Manzil: Сам.Тайлок.Курганча ЗАВОД ТОННИ ГРИНН
"""
NEWS_TEXT = """Barcha yangiliklarni kuzatib boring"""

BTN_PRODUCTS = "📦 Mahsulotlar"
BTN_MY_ORDERS = "📄 Mening buyurtmalarim"
BTN_INFO = "ℹ️ Ma'lumot"
BTN_CONTACT = "📞 Aloqa"
BTN_NEWS = "📰 Yangiliklar"
BTN_STATS = "📊 Statistika"
BTN_ORDERS_LIST = "🧾 Buyurtmalar ro'yxati"
BTN_BROADCAST = "📣 Xabar tarqatish"
BTN_ADD_PRODUCT = "➕ Mahsulot qo'shish"
BTN_EDIT_PRODUCT = "✏️ Mahsulotni tahrirlash"
BTN_REPORTS = "📑 Hisobotlar"
BTN_SEND_PHONE = "📲 Telefon raqamni yuborish"
BTN_CANCEL = "❌ Bekor qilish"
BTN_SEND_LOCATION = "📍 Lokatsiyani yuborish"
BTN_SUPPORT = "🆘 Qo'llab-quvvatlash"
BTN_SKIP_DESCRIPTION = "⏭ O'tkazish"
BTN_SKIP_PHOTOS = "⏭ O'tkazish"
BTN_BLOCK_USERS = "🚫 Bloklash/ochish"
BTN_BLOCK = "🔒 Bloklash"
BTN_UNBLOCK = "🔓 Blokdan chiqarish"
BTN_CREATE_ORDER = "📝 Buyurtma yaratish"


class OrderStates(StatesGroup):
    quantity = State()
    address = State()
    confirm = State()


class AdminOrderStates(StatesGroup):
    name = State()
    phone = State()
    address = State()
    product = State()
    quantity = State()
    confirm = State()


class AddProductStates(StatesGroup):
    name = State()
    price = State()
    description = State()
    photos = State()


class EditProductStates(StatesGroup):
    field = State()
    value = State()
    photos = State()


class BroadcastStates(StatesGroup):
    content = State()
    confirm = State()


class OrderSearchStates(StatesGroup):
    order_id = State()


class OrderDeleteStates(StatesGroup):
    order_id = State()
    confirm = State()


class SupportStates(StatesGroup):
    waiting_message = State()


class BlockUserStates(StatesGroup):
    action = State()
    phone = State()


class ReportStates(StatesGroup):
    start_date = State()
    end_date = State()


@dataclass
class BroadcastPayload:
    kind: str
    text: Optional[str] = None
    file_ids: Optional[list[str]] = None
    caption: Optional[str] = None
    media_items: Optional[list[dict[str, str]]] = None


media_group_buffer: dict[int, dict[str, object]] = {}
support_reply_map: dict[tuple[int, int], int] = {}
support_media_group_reject: set[tuple[int, str]] = set()
admin_media_group_reject: set[tuple[int, str]] = set()


class ActivityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, types.Message) and event.from_user:
            if event.chat.type != "private":
                if (
                    event.chat.id in GROUP_LIST
                    and event.reply_to_message
                    and is_admin(event.from_user.id)
                ):
                    return await handler(event, data)
                return
            db.update_last_active(event.from_user.id)
        return await handler(event, data)


class BlockedUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, (types.Message, types.CallbackQuery)) and event.from_user:
            if not is_admin(event.from_user.id) and db.is_user_blocked(event.from_user.id):
                blocked_text = (
                    "⛔️ Siz bloklangansiz.\n"
                    "Admin tomonidan blokdan chiqarilgach botdan foydalanishingiz mumkin."
                )
                if isinstance(event, types.CallbackQuery):
                    await event.answer(blocked_text, show_alert=True)
                else:
                    await event.answer(blocked_text)
                return
        return await handler(event, data)


def can_view_reports(user_id: int) -> bool:
    return user_id in REPORT_LIST


def user_keyboard(user_id: int, is_admin_override: Optional[bool] = None) -> ReplyKeyboardMarkup:
    is_admin_user = is_admin_override if is_admin_override is not None else is_admin(user_id)
    rows = [
        [KeyboardButton(text=BTN_PRODUCTS)],
        [KeyboardButton(text=BTN_CONTACT), KeyboardButton(text=BTN_NEWS)],
    ]
    if not is_admin_user:
        rows.insert(1, [KeyboardButton(text=BTN_MY_ORDERS)])
        rows.append([KeyboardButton(text=BTN_SUPPORT)])
    if is_admin_user:
        rows.append([KeyboardButton(text=BTN_STATS), KeyboardButton(text=BTN_ORDERS_LIST)])
        rows.append([KeyboardButton(text=BTN_CREATE_ORDER)])
    if can_view_reports(user_id):
        rows.append([KeyboardButton(text=BTN_REPORTS)])
    if is_admin_user:
        rows.append([KeyboardButton(text=BTN_BROADCAST)])
        rows.append([KeyboardButton(text=BTN_BLOCK_USERS)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_SEND_PHONE, request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def add_product_photos_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SKIP_PHOTOS)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def block_action_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BLOCK), KeyboardButton(text=BTN_UNBLOCK)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def description_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_SKIP_DESCRIPTION)], [KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def order_address_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SEND_LOCATION, request_location=True)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def is_cancel_message(message: types.Message) -> bool:
    return bool(message.text and message.text.strip() == BTN_CANCEL)


def normalize_phone(value: str) -> str:
    digits = "".join(char for char in value if char.isdigit())
    if digits.startswith("998") and len(digits) >= 12:
        return digits[-9:]
    return digits


def find_user_by_phone(value: str) -> Optional[sqlite3.Row]:
    normalized = normalize_phone(value)
    if not normalized:
        return None
    for user in db.list_users():
        user_phone = normalize_phone(user["phone"] or "")
        if user_phone == normalized:
            return user
    return None


def format_user_contact(first_name: Optional[str], last_name: Optional[str], phone: Optional[str]) -> str:
    name_parts = [part for part in [first_name, last_name] if part]
    full_name = " ".join(name_parts) if name_parts else "Noma'lum foydalanuvchi"
    phone_display = phone or "📞 Telefon yo'q"
    return f"{full_name} ({phone_display})"


def format_user_name(first_name: Optional[str], last_name: Optional[str]) -> str:
    name_parts = [part for part in [first_name, last_name] if part]
    return " ".join(name_parts) if name_parts else "Noma'lum foydalanuvchi"


async def cancel_admin_action(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Amal bekor qilindi.", reply_markup=user_keyboard(message.from_user.id))


def product_inline_keyboard(product_id: int, admin: bool) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🛒 Sotib olish uchun ariza yuborish", callback_data=f"order:{product_id}")]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="✏️ Tahrirlash", callback_data=f"edit:{product_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def edit_inline_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✏️ Tahrirlash", callback_data=f"edit:{product_id}")]]
    )


def edit_fields_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Nomi", callback_data="field:name")],
            [InlineKeyboardButton(text="💰 Narxi", callback_data="field:price")],
            [InlineKeyboardButton(text="🗒 Tavsif", callback_data="field:description")],
            [InlineKeyboardButton(text="🖼 Rasmlar", callback_data="field:photos")],
            [InlineKeyboardButton(text="🗑 O'chirish", callback_data="field:delete")],
        ]
    )


def news_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Telegram", url="https://t.me/shrotsavdo")],
            [InlineKeyboardButton(text="Instagram", url="https://instagram.com/shrotsavdo")],
        ]
    )


def delete_product_confirm_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Ha, o'chirish", callback_data=f"product_delete:confirm:{product_id}"
                )
            ],
            [InlineKeyboardButton(text="↩️ Yo'q", callback_data=f"product_delete:cancel:{product_id}")],
        ]
    )


def orders_status_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🟢 Yopilmagan statuslar", callback_data="orders:open"),
                InlineKeyboardButton(text="✅ Yopilgan statuslar", callback_data="orders:closed:0"),
            ],
            [InlineKeyboardButton(text="❌ Bekor qilingan statuslar", callback_data="orders:canceled:0")],
            [InlineKeyboardButton(text="🔎 ID bo'yicha qidirish", callback_data="orders:search")],
            [InlineKeyboardButton(text="🗑 Buyurtmani o'chirish", callback_data="orders:delete")],
        ]
    )


def order_action_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Qabul qilish va yopish", callback_data=f"orders:close:{order_id}")],
            [InlineKeyboardButton(text="❌ Bekor qilish va yopish", callback_data=f"orders:cancel:{order_id}")],
        ]
    )


def order_cancel_confirm_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, bekor qilish", callback_data=f"orders:cancel_confirm:{order_id}")],
            [InlineKeyboardButton(text="↩️ Yo'q", callback_data=f"orders:cancel_keep:{order_id}")],
        ]
    )


def order_delete_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, o'chirish", callback_data="orders:delete_confirm")],
            [InlineKeyboardButton(text="↩️ Yo'q", callback_data="orders:delete_keep")],
        ]
    )


def user_order_action_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Buyurtmani bekor qilish", callback_data=f"user_orders:cancel:{order_id}")]
        ]
    )


def user_order_cancel_confirm_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, bekor qilish", callback_data=f"user_orders:cancel_confirm:{order_id}")],
            [InlineKeyboardButton(text="↩️ Yo'q", callback_data=f"user_orders:cancel_keep:{order_id}")],
        ]
    )


def order_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Buyurtmani tasdiqlash", callback_data="order_confirm")],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="order_cancel")],
        ]
    )


def admin_order_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha", callback_data="admin_order_confirm")],
            [InlineKeyboardButton(text="❌ Yo'q", callback_data="admin_order_cancel")],
        ]
    )


def admin_order_products_keyboard(products: list[sqlite3.Row]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=product["name"], callback_data=f"admin_order_product:{product['id']}")]
        for product in products
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_location_link(latitude: Optional[float], longitude: Optional[float]) -> Optional[str]:
    if latitude is None or longitude is None:
        return None
    return f"https://www.google.com/maps?q={latitude},{longitude}"


def format_order_message(order, include_id: bool = True, include_address: bool = True) -> str:
    person = escape(format_order_person(order["first_name"], order["last_name"]))
    created_at = escape(format_order_datetime(order["created_at"]))
    price_per_kg = order["order_price_per_kg"] or order["product_price_per_kg"]
    lines = []
    if include_id:
        lines.append(f"🆔 ID: {escape(str(order['id']))}")
    lines.extend(
        [
            f"👤 Ism: {person}",
            f"📦 Mahsulot: {escape(order['product_name'])}",
            f"⚖️ Miqdor: {escape(order['quantity'])}",
            f"💰 Narx (1 kg, ariza vaqti): {escape(format_price(price_per_kg))} сум",
            f"💵 Jami: {escape(format_deal_price(order['quantity'], price_per_kg))}",
            f"📞 Telefon: {escape(order['phone'] or 'Kiritilmagan')}",
        ]
    )
    if include_address:
        lines.append(f"📍 Manzil: {escape(order['address'])}")
        location_link = format_location_link(order["latitude"], order["longitude"])
        if location_link:
            lines.append(f"🗺 Lokatsiya: <a href=\"{escape(location_link)}\">Manzilga utish</a>")
    lines.append(f"📅 Sana: {created_at}")
    return "\n".join(lines)


def format_status_label(status: str, canceled_by_role: Optional[str]) -> str:
    if status == "open":
        return "🟢 Ochiq"
    if status == "closed":
        return "✅ Qabul qilingan va yopilgan"
    if status == "canceled" and canceled_by_role == "user":
        return "❌ Bekor qilish va yopish"
    if status == "canceled" and canceled_by_role == "admin":
        return "⚠️ Admin tomonidan bekor qilingan"
    if status == "canceled":
        return "❌ Bekor qilingan"
    return status


def format_admin_order_details(order) -> str:
    status_label = format_status_label(order["status"], order["canceled_by_role"])
    return "\n".join(
        [
            format_order_message(order, include_id=True, include_address=True),
            f"📌 Holati: {escape(status_label)}",
        ]
    )


def format_user_order_message(order) -> str:
    created_at = escape(format_order_datetime(order["created_at"]))
    price_per_kg = order["order_price_per_kg"] or order["product_price_per_kg"]
    status_label = format_status_label(order["status"], order["canceled_by_role"])
    lines = [
        f"🆔 ID: {escape(str(order['id']))}",
        f"📦 Mahsulot: {escape(order['product_name'])}",
        f"⚖️ Miqdor: {escape(order['quantity'])}",
        f"💰 Narx (1 kg, ariza vaqti): {escape(format_price(price_per_kg))} сум",
        f"💵 Jami: {escape(format_deal_price(order['quantity'], price_per_kg))}",
        f"📍 Manzil: {escape(order['address'])}",
        f"📌 Holati: {escape(status_label)}",
        f"📅 Sana: {created_at}",
    ]
    return "\n".join(lines)


async def notify_admins_new_order(bot: Bot, order_id: int) -> None:
    order = db.get_order_with_details(order_id)
    if not order:
        return
    text = "🆕 Yangi ariza:\n" + format_order_message(order)
    for admin_id in ADMIN_LIST:
        try:
            await bot.send_message(
                admin_id,
                text,
                reply_markup=order_action_keyboard(order_id),
                parse_mode="HTML",
            )
        except Exception:
            continue


def format_order_datetime(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


def format_order_person(first_name: Optional[str], last_name: Optional[str]) -> str:
    parts = [part for part in [first_name, last_name] if part]
    return " ".join(parts) if parts else "👤 Noma'lum"


def format_price(value: Optional[float]) -> str:
    if value is None:
        return "⚠️ Kiritilmagan"
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def format_money_with_commas(value: Optional[float]) -> str:
    if value is None:
        return "⚠️ Kiritilmagan"
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def parse_report_date(value: str) -> Optional[datetime]:
    cleaned = value.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def format_report_period(start_date: datetime, end_date: datetime) -> str:
    return f"{start_date.strftime('%Y-%m-%d')} — {end_date.strftime('%Y-%m-%d')}"


def format_tons(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def calculate_report_stats(
    rows: list[sqlite3.Row],
) -> tuple[float, float, list[dict[str, object]]]:
    per_client_product: dict[tuple[int, str], dict[str, object]] = {}
    total_sum = 0.0
    total_tons = 0.0
    for row in rows:
        qty_kg = parse_quantity_to_kg(row["quantity"])
        price_per_kg = row["order_price_per_kg"] or row["product_price_per_kg"]
        if qty_kg is None or price_per_kg is None:
            continue
        amount = qty_kg * price_per_kg
        tons = qty_kg / 1000
        total_sum += amount
        total_tons += tons
        key = (row["user_id"], row["product_name"])
        entry = per_client_product.setdefault(
            key,
            {
                "name": format_user_contact(row["first_name"], row["last_name"], row["phone"]),
                "product": row["product_name"],
                "tons": 0.0,
                "amount": 0.0,
            },
        )
        entry["tons"] += tons
        entry["amount"] += amount
    sorted_entries = sorted(
        per_client_product.values(),
        key=lambda item: item["amount"],
        reverse=True,
    )
    return total_sum, total_tons, sorted_entries


def build_report_summary_text(
    rows: list[sqlite3.Row],
    start_date: datetime,
    end_date: datetime,
    limit: int = 20,
) -> str:
    total_sum, total_tons, entries = calculate_report_stats(rows)
    period_label = format_report_period(start_date, end_date)
    total_sum_label = format_money_with_commas(total_sum)
    total_tons_label = format_tons(total_tons)
    lines = [
        "📑 Hisobot",
        f"📅 Davr: {period_label}",
        f"💵 Umumiy summa: {total_sum_label} so'm",
        f"⚖️ Jami tonna: {total_tons_label} t",
    ]
    if not entries:
        lines.append("📭 Ma'lumot topilmadi.")
        return "\n".join(lines)
    lines.append("📌 Mijozlar va mahsulotlar:")
    for idx, entry in enumerate(entries[:limit], start=1):
        amount_label = format_money_with_commas(entry["amount"])
        lines.append(
            f"{idx}. {entry['name']} — {entry['product']}: "
            f"{format_tons(entry['tons'])} t, {amount_label} so'm"
        )
    return "\n".join(lines)


def build_report_html(
    rows: list[sqlite3.Row],
    start_date: datetime,
    end_date: datetime,
) -> str:
    total_sum, total_tons, entries = calculate_report_stats(rows)
    rows_html = []
    for idx, entry in enumerate(entries, start=1):
        rows_html.append(
            "<tr>"
            f"<td data-label=\"#\">{idx}</td>"
            f"<td data-label=\"Mijoz\" data-key=\"name\" data-value=\"{escape(entry['name'])}\">"
            f"{escape(entry['name'])}"
            "</td>"
            f"<td data-label=\"Mahsulot\" data-key=\"product\" data-value=\"{escape(entry['product'])}\">"
            f"{escape(entry['product'])}"
            "</td>"
            "<td data-label=\"Tonna (t)\" data-key=\"tons\" "
            f"data-value=\"{entry['tons']}\" style=\"text-align:right;\">"
            f"{escape(format_tons(entry['tons']))}"
            "</td>"
            "<td data-label=\"Jami summa (so'm)\" data-key=\"amount\" "
            f"data-value=\"{entry['amount']}\" style=\"text-align:right;\">"
            f"{escape(format_money_with_commas(entry['amount']))}"
            "</td>"
            "</tr>"
        )
    if not rows_html:
        rows_html.append(
            "<tr><td colspan=\"5\" style=\"text-align:center; padding: 16px;\">"
            "Ma'lumot topilmadi"
            "</td></tr>"
        )
    period_label = format_report_period(start_date, end_date)
    total_sum_label = escape(format_money_with_commas(total_sum))
    total_tons_label = escape(format_tons(total_tons))
    return f"""<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Hisobot</title>
  <style>
    body {{
      font-family: "Segoe UI", Arial, sans-serif;
      background: #f5f7fb;
      color: #1f2a44;
      margin: 0;
      padding: 24px;
    }}
    * {{
      box-sizing: border-box;
    }}
    .card {{
      max-width: 900px;
      margin: 0 auto;
      background: #ffffff;
      border-radius: 16px;
      box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 24px;
    }}
    .period {{
      color: #64748b;
      margin-bottom: 16px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 16px;
    }}
    th, td {{
      padding: 12px;
      border-bottom: 1px solid #e2e8f0;
      font-size: 14px;
    }}
    th {{
      text-align: left;
      background: #f1f5f9;
      color: #475569;
    }}
    th.sortable {{
      cursor: pointer;
      user-select: none;
    }}
    .toolbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin: 12px 0 4px;
    }}
    .sort-buttons {{
      display: none;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .sort-button {{
      padding: 8px 12px;
      border-radius: 10px;
      border: 1px solid #e2e8f0;
      background: #f8fafc;
      font-size: 12px;
      cursor: pointer;
    }}
    .search-input {{
      flex: 1 1 240px;
      padding: 10px 12px;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      font-size: 14px;
    }}
    .hint {{
      font-size: 12px;
      color: #64748b;
    }}
    td[data-label]::before {{
      content: attr(data-label);
      display: none;
      font-weight: 600;
      color: #475569;
    }}
    .total {{
      margin-top: 20px;
      padding: 16px;
      background: #0f172a;
      color: #f8fafc;
      border-radius: 12px;
      text-align: right;
      font-weight: 600;
    }}
    @media (max-width: 600px) {{
      body {{
        padding: 16px;
      }}
      .card {{
        padding: 16px;
      }}
      table {{
        border: 0;
      }}
      thead {{
        display: none;
      }}
      tr {{
        display: block;
        margin-bottom: 12px;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 8px;
      }}
      td {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        padding: 8px 6px;
        border: none;
      }}
      td[data-label]::before {{
        display: block;
      }}
      .total {{
        text-align: left;
      }}
      .toolbar {{
        flex-direction: column;
        align-items: stretch;
      }}
      .search-input {{
        font-size: 12px;
        padding: 6px 8px;
        line-height: 1.2;
      }}
      .sort-buttons {{
        display: flex;
      }}
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Hisobot</h1>
    <div class="period">Davr: {escape(period_label)}</div>
    <div class="toolbar">
      <input id="searchInput" class="search-input" type="text" placeholder="Qidirish: mijoz, mahsulot, tonna yoki summa" />
      <div class="hint">Sarlavhalarni bosib saralang (Mijoz, Mahsulot, Tonna, Jami summa)</div>
    </div>
    <div class="sort-buttons" aria-label="Saralash tugmalari">
      <button type="button" class="sort-button" data-sort="name">Mijoz</button>
      <button type="button" class="sort-button" data-sort="product">Mahsulot</button>
      <button type="button" class="sort-button" data-sort="tons">Tonna</button>
      <button type="button" class="sort-button" data-sort="amount">Jami summa</button>
    </div>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th class="sortable" data-sort="name">Mijoz</th>
          <th class="sortable" data-sort="product">Mahsulot</th>
          <th class="sortable" data-sort="tons" style="text-align:right;">Tonna (t)</th>
          <th class="sortable" data-sort="amount" style="text-align:right;">Jami summa (so'm)</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    <div class="total">Umumiy summa: {total_sum_label} so'm · Jami tonna: {total_tons_label} t</div>
  </div>
  <script>
    const tbody = document.querySelector("tbody");
    const rows = Array.from(tbody.querySelectorAll("tr"));
    const totalElement = document.querySelector(".total");
    const sortState = {{ name: "desc", product: "asc", tons: "asc", amount: "asc" }};

    const getCellValue = (row, key) => {{
      const cell = row.querySelector(`[data-key="${{key}}"]`);
      if (!cell) return "";
      if (key === "name" || key === "product") {{
        return (cell.dataset.value || cell.textContent).trim().toLowerCase();
      }}
      const num = parseFloat(cell.dataset.value || "0");
      return Number.isNaN(num) ? 0 : num;
    }};

    const formatNumber = (value) => {{
      if (Math.abs(value - Math.round(value)) < 1e-9) {{
        return Math.round(value).toLocaleString("en-US");
      }}
      return value.toLocaleString("en-US", {{
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      }});
    }};

    const updateTotals = () => {{
      let sum = 0;
      let tons = 0;
      rows.forEach((row) => {{
        if (row.style.display === "none") {{
          return;
        }}
        const amountCell = row.querySelector('[data-key="amount"]');
        const tonsCell = row.querySelector('[data-key="tons"]');
        if (!amountCell || !tonsCell) {{
          return;
        }}
        const amountValue = parseFloat(amountCell.dataset.value || "0");
        const tonsValue = parseFloat(tonsCell.dataset.value || "0");
        if (!Number.isNaN(amountValue)) {{
          sum += amountValue;
        }}
        if (!Number.isNaN(tonsValue)) {{
          tons += tonsValue;
        }}
      }});
      totalElement.textContent = `Umumiy summa: ${{formatNumber(sum)}} so'm · Jami tonna: ${{formatNumber(tons)}} t`;
    }};

    const sortRows = (key) => {{
      const direction = sortState[key] === "asc" ? "desc" : "asc";
      sortState[key] = direction;
      rows.sort((a, b) => {{
        const av = getCellValue(a, key);
        const bv = getCellValue(b, key);
        if (key === "name" || key === "product") {{
          if (av < bv) return direction === "asc" ? -1 : 1;
          if (av > bv) return direction === "asc" ? 1 : -1;
          return 0;
        }}
        return direction === "asc" ? av - bv : bv - av;
      }});
      rows.forEach((row) => tbody.appendChild(row));
    }};

    const bindSort = (selector) => {{
      document.querySelectorAll(selector).forEach((element) => {{
        element.addEventListener("click", () => sortRows(element.dataset.sort));
      }});
    }};

    bindSort("th[data-sort]");
    bindSort("button[data-sort]");

    const searchInput = document.getElementById("searchInput");
    searchInput.addEventListener("input", (event) => {{
      const query = event.target.value.trim().toLowerCase();
      rows.forEach((row) => {{
        const rowText = row.textContent.toLowerCase();
        row.style.display = rowText.includes(query) ? "" : "none";
      }});
      updateTotals();
    }});

    updateTotals();
  </script>
</body>
</html>"""


def parse_quantity_to_kg(value: str) -> Optional[float]:
    cleaned = value.strip().lower()
    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", cleaned)
    if not match:
        return None
    number = float(match.group(1).replace(",", "."))
    if any(unit in cleaned for unit in ["tonna", "t"]):
        return number * 1000
    return number


def parse_quantity_to_tons(value: str) -> Optional[float]:
    cleaned = value.strip()
    if not re.fullmatch(r"\d+(?:[.,]\d+)?", cleaned):
        return None
    return float(cleaned.replace(",", "."))


def format_deal_price(quantity: str, price_per_kg: Optional[float]) -> str:
    if price_per_kg is None:
        return "⚠️ Hisoblab bo'lmadi"
    qty_kg = parse_quantity_to_kg(quantity)
    if qty_kg is None:
        return "⚠️ Hisoblab bo'lmadi"
    return f"{format_money_with_commas(qty_kg * price_per_kg)} сум"


def report_period_keyboard() -> InlineKeyboardMarkup:
    today = datetime.now(TASHKENT_TZ)
    current_month_label = today.strftime("%Y-%m")
    prev_month = today.replace(day=1) - timedelta(days=1)
    prev_month_label = prev_month.strftime("%Y-%m")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"📅 Joriy oy ({current_month_label})",
                    callback_data="report_period:current_month",
                ),
                InlineKeyboardButton(
                    text=f"📅 Oldingi oy ({prev_month_label})",
                    callback_data="report_period:previous_month",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"📅 {today.year} yil",
                    callback_data="report_period:current_year",
                ),
                InlineKeyboardButton(
                    text=f"📅 {today.year - 1} yil",
                    callback_data="report_period:previous_year",
                ),
            ],
        ]
    )


def get_month_range(reference: datetime, offset: int) -> tuple[datetime, datetime]:
    year = reference.year
    month = reference.month + offset
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    start = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    end = next_month - timedelta(days=1)
    return start, end


def get_year_range(year: int) -> tuple[datetime, datetime]:
    return datetime(year, 1, 1), datetime(year, 12, 31)


async def send_report_for_period(
    bot: Bot,
    chat_id: int,
    user_id: int,
    start_date: datetime,
    end_date: datetime,
) -> None:
    async def prepare_report_payload() -> tuple[str, str, str]:
        return await run_in_thread(
            build_report_payload,
            user_id,
            start_date,
            end_date,
        )

    loading_message = None
    file_path: Optional[str] = None
    try:
        loading_message = await bot.send_message(chat_id, "⏳ Hisobot tayyorlanmoqda...")
        summary_text, period_label, file_path = await prepare_report_payload()
        await bot.send_message(
            chat_id,
            summary_text,
            reply_markup=user_keyboard(user_id),
        )
        await bot.send_document(
            chat_id,
            types.FSInputFile(file_path),
            caption=f"📑 Hisobot tayyor.\nDavr: {period_label}",
            reply_markup=user_keyboard(user_id),
        )
    except Exception:
        logging.exception("Failed to generate report payload.")
        await bot.send_message(
            chat_id,
            "❌ Hisobot tayyorlashda xatolik yuz berdi. Keyinroq urinib ko'ring.",
            reply_markup=user_keyboard(user_id),
        )
    finally:
        if loading_message:
            try:
                await loading_message.delete()
            except Exception:
                logging.exception("Failed to delete loading message.")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


def build_report_payload(
    user_id: int,
    start_date: datetime,
    end_date: datetime,
) -> tuple[str, str, str]:
    rows = list(
        db.list_orders_for_report(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
    )
    summary_text = build_report_summary_text(rows, start_date, end_date)
    period_label = format_report_period(start_date, end_date)
    report_html = build_report_html(rows, start_date, end_date)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        suffix=".html",
        prefix=f"report_{user_id}_",
        delete=False,
    ) as handle:
        handle.write(report_html)
        file_path = handle.name
    return summary_text, period_label, file_path


async def send_product(chat_id: int, product, bot: Bot, admin: bool) -> None:
    photos = db.get_product_photos(product["id"])
    caption = (
        f"📦 Mahsulot: {product['name']}\n"
        f"💰 Narxi (1 kg): {product['price_per_kg']} сум\n"
        f"🗒 Tavsif: {product['description'] or 'Kiritilmagan'}"
    )
    if photos:
        await bot.send_photo(
            chat_id=chat_id,
            photo=photos[0],
            caption=caption,
            reply_markup=product_inline_keyboard(product["id"], admin),
        )
        remaining_photos = photos[1:3]
        if len(remaining_photos) == 1:
            await bot.send_photo(chat_id=chat_id, photo=remaining_photos[0])
        elif len(remaining_photos) > 1:
            builder = MediaGroupBuilder()
            for file_id in remaining_photos:
                builder.add_photo(media=file_id)
            await bot.send_media_group(chat_id=chat_id, media=builder.build())
    else:
        await bot.send_message(
            chat_id=chat_id,
            text=caption,
            reply_markup=product_inline_keyboard(product["id"], admin),
        )


async def ensure_user_registered(message: types.Message) -> bool:
    user = db.get_user_by_tg_id(message.from_user.id)
    if not user or not user["phone"]:
        await message.answer(
            "📲 Iltimos, botdan foydalanish uchun telefon raqamingizni yuboring.",
            reply_markup=contact_keyboard(),
        )
        return False
    return True


def format_support_user_details(
    name_display: str,
    phone: Optional[str],
    text: Optional[str],
) -> str:
    phone_display = phone or "Telefon yo'q"
    user_text = text.strip() if text else "—"
    return (
        "🆘 Yangi qo'llab-quvvatlash so'rovi\n"
        f"👤 Foydalanuvchi: {name_display}\n"
        f"📞 Telefon: {phone_display}\n\n"
        f"Text: {user_text}\n\n"
        "↩️ Javob berish uchun shu xabarga reply qiling."
    )


def parse_support_user_id(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"\bID:\s*(\d+)\b", text)
    if not match:
        return None
    return int(match.group(1))


async def handle_media_group_timeout(user_id: int, bot: Bot, state: FSMContext) -> None:
    await asyncio.sleep(1.2)
    buffer_entry = media_group_buffer.get(user_id)
    if not buffer_entry or buffer_entry.get("finalized"):
        return
    buffer_entry["finalized"] = True
    payload = BroadcastPayload(
        kind="media_group",
        caption=buffer_entry.get("caption"),
        media_items=buffer_entry["media_items"],
    )
    await state.update_data(broadcast_payload=payload)
    await bot.send_message(user_id, "📣 Tarqatmani tasdiqlaysizmi? (Ha/Yo'q)")
    await state.set_state(BroadcastStates.confirm)


def parse_price(value: str) -> Optional[float]:
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_LIST


def safe_caption(message: types.Message) -> Optional[str]:
    return message.caption if message.caption else None


async def reverse_geocode(latitude: float, longitude: float) -> Optional[str]:
    def _lookup() -> Optional[str]:
        params = urllib.parse.urlencode(
            {
                "format": "json",
                "lat": latitude,
                "lon": longitude,
                "zoom": 18,
                "addressdetails": 1,
            }
        )
        url = f"https://nominatim.openstreetmap.org/reverse?{params}"
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "shrotdef-bot/1.0"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload.get("display_name")

    try:
        return await run_in_thread(_lookup)
    except Exception:
        return None


async def run_in_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    token = "8396669139:AAFvr8gWi7uXDMwPLBePF9NmYf16wsHmtPU"
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    db.init_db()


    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.middleware(BlockedUserMiddleware())
    dp.callback_query.middleware(BlockedUserMiddleware())
    dp.message.middleware(ActivityMiddleware())

    @dp.message(CommandStart())
    async def start(message: types.Message) -> None:
        db.add_or_update_user(
            message.from_user.id,
            message.from_user.first_name,
            message.from_user.last_name,
        )
        user = db.get_user_by_tg_id(message.from_user.id)
        if user and user["phone"]:
            await message.answer(
                "👋 Xush kelibsiz!", reply_markup=user_keyboard(message.from_user.id)
            )
        else:
            await message.answer(
                "👋 Assalomu alaykum! Botdan foydalanish uchun telefon raqamingizni yuboring.",
                reply_markup=contact_keyboard(),
            )

    @dp.message(F.contact)
    async def handle_contact(message: types.Message) -> None:
        if not message.contact or message.contact.user_id != message.from_user.id:
            await message.answer("⚠️ Iltimos, o'zingizning raqamingizni yuboring.")
            return
        db.update_user_phone(message.from_user.id, message.contact.phone_number)
        await message.answer(
            "✅ Rahmat! Endi botdan foydalanishingiz mumkin.",
            reply_markup=user_keyboard(message.from_user.id),
        )

    @dp.message(F.text == BTN_PRODUCTS)
    async def show_products(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        admin = is_admin(message.from_user.id)
        products = db.list_products()
        if not products:
            await message.answer("📭 Hozircha mahsulotlar mavjud emas.")
            if admin:
                await message.answer(
                    "➕ Mahsulot qo'shish uchun pastdagi tugmani bosing.",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text=BTN_ADD_PRODUCT, callback_data="add_product")]
                        ]
                    ),
                )
            return
        for product in products:
            await send_product(message.chat.id, product, bot, admin)
        if admin:
            await message.answer(
                "➕ Mahsulot qo'shish uchun pastdagi tugmani bosing.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text=BTN_ADD_PRODUCT, callback_data="add_product")]
                    ]
                ),
            )

    @dp.callback_query(F.data.startswith("order:"))
    async def order_start(callback: types.CallbackQuery, state: FSMContext) -> None:
        product_id = int(callback.data.split(":", 1)[1])
        await state.update_data(product_id=product_id)
        await callback.message.answer(
            "⚖️ Necha tonna kerak? (masalan: 2.3 yoki 2,3)\n"
            "📌 Minimal buyurtma: 2 tonna.",
            reply_markup=cancel_keyboard(),
        )
        await state.set_state(OrderStates.quantity)
        await callback.answer()

    @dp.message(OrderStates.quantity)
    async def order_quantity(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer(
                "❌ Ariza bekor qilindi.", reply_markup=user_keyboard(message.from_user.id)
            )
            return
        qty_tons = parse_quantity_to_tons(message.text or "")
        if qty_tons is None:
            await message.answer(
                "⚠️ Miqdorni to'g'ri kiriting (faqat raqamlar: 2, 2.3 yoki 2,3).",
                reply_markup=cancel_keyboard(),
            )
            return
        if qty_tons < 2:
            await message.answer(
                "⚠️ Minimal buyurtma 2 tonna. Iltimos, qayta kiriting.",
                reply_markup=cancel_keyboard(),
            )
            return
        normalized_quantity = f"{format_price(qty_tons)} tonna"
        await state.update_data(quantity=normalized_quantity)
        await message.answer(
            "📍 Manzilni kiriting yoki lokatsiyani yuboring.",
            reply_markup=order_address_keyboard(),
        )
        await state.set_state(OrderStates.address)

    async def send_order_confirmation(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        user = db.get_user_by_tg_id(message.from_user.id)
        if not user:
            await message.answer("❌ Foydalanuvchi topilmadi.")
            await state.clear()
            return
        product = db.get_product(data["product_id"])
        if not product:
            await message.answer("❌ Mahsulot topilmadi.")
            await state.clear()
            return
        address = data.get("address")
        if not address:
            await message.answer("⚠️ Manzil topilmadi, qayta kiriting.")
            await state.clear()
            return
        quantity = data["quantity"]
        price_per_kg = product["price_per_kg"]
        location_link = format_location_link(data.get("latitude"), data.get("longitude"))
        lines = [
            "Buyurtma ma'lumotlari:",
            f"📦 Mahsulot: {escape(product['name'])}",
            f"⚖️ Miqdor: {escape(quantity)}",
            f"💰 Narx (1 kg): {escape(format_price(price_per_kg))} сум",
            f"💵 Jami: {escape(format_deal_price(quantity, price_per_kg))}",
            f"📍 Manzil: {escape(address)}",
        ]
        if location_link:
            lines.append(f"🗺 Lokatsiya: <a href=\"{escape(location_link)}\">Manzilga utish</a>")
        lines.append("✅ Buyurtmani tasdiqlaysizmi?")
        await message.answer(
            "\n".join(lines),
            reply_markup=order_confirm_keyboard(),
            parse_mode="HTML",
        )
        await state.set_state(OrderStates.confirm)

    async def finalize_order(message: types.Message, state: FSMContext, user_id: int) -> None:
        data = await state.get_data()
        user = db.get_user_by_tg_id(user_id)
        if not user:
            await message.answer("❌ Foydalanuvchi topilmadi.")
            await state.clear()
            return
        product = db.get_product(data["product_id"])
        if not product:
            await message.answer("❌ Mahsulot topilmadi.")
            await state.clear()
            return
        address = data.get("address")
        if not address:
            await message.answer("⚠️ Manzil topilmadi, qayta kiriting.")
            await state.clear()
            return
        order_id = db.add_order(
            user["id"],
            data["product_id"],
            data["quantity"],
            address,
            product["price_per_kg"],
            latitude=data.get("latitude"),
            longitude=data.get("longitude"),
        )
        await message.answer(
            "✅ Buyurtma tasdiqlandi!", reply_markup=user_keyboard(message.from_user.id)
        )
        await notify_admins_new_order(message.bot, order_id)
        await state.clear()

    @dp.message(OrderStates.address, F.location)
    async def order_address_location(message: types.Message, state: FSMContext) -> None:
        location = message.location
        if not location:
            await message.answer("⚠️ Lokatsiya topilmadi, qayta yuboring.")
            return
        address_text = await reverse_geocode(location.latitude, location.longitude)
        if not address_text:
            address_text = "📍 Lokatsiya yuborildi"
        await state.update_data(
            address=address_text,
            latitude=location.latitude,
            longitude=location.longitude,
        )
        await send_order_confirmation(message, state)

    @dp.message(OrderStates.address)
    async def order_address(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer(
                "❌ Ariza bekor qilindi.", reply_markup=user_keyboard(message.from_user.id)
            )
            return
        await state.update_data(address=message.text, latitude=None, longitude=None)
        await send_order_confirmation(message, state)

    @dp.callback_query(F.data == "order_confirm")
    async def confirm_order(callback: types.CallbackQuery, state: FSMContext) -> None:
        current_state = await state.get_state()
        if current_state != OrderStates.confirm:
            await callback.answer()
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await finalize_order(callback.message, state, callback.from_user.id)
        await callback.answer("✅ Buyurtma tasdiqlandi")

    @dp.callback_query(F.data == "order_cancel")
    async def cancel_order(callback: types.CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(
                "❌ Buyurtma bekor qilindi.", reply_markup=user_keyboard(callback.from_user.id)
            )
        await callback.answer("❌ Bekor qilindi")

    @dp.message(F.text == BTN_CREATE_ORDER)
    async def start_admin_order(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await state.clear()
        await message.answer("📞 Mijoz telefon raqamini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AdminOrderStates.phone)

    @dp.message(AdminOrderStates.name)
    async def admin_order_name(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        name = (message.text or "").strip()
        if not name:
            await message.answer("⚠️ Iltimos, mijoz ismini kiriting.", reply_markup=cancel_keyboard())
            return
        await state.update_data(client_name=name)
        await message.answer("📍 Mijoz manzilini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AdminOrderStates.address)

    @dp.message(AdminOrderStates.phone)
    async def admin_order_phone(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        phone = (message.text or "").strip()
        normalized_phone = normalize_phone(phone)
        if not normalized_phone:
            await message.answer("⚠️ Telefon raqamini kiriting.", reply_markup=cancel_keyboard())
            return
        user = find_user_by_phone(phone)
        if user:
            client_name = format_user_name(user["first_name"], user["last_name"])
            await state.update_data(
                user_id=user["id"],
                client_tg_id=user["tg_id"],
                client_name=client_name,
                client_phone=user["phone"] or phone,
            )
            await message.answer(
                f"✅ Mijoz topildi: {client_name}. 📍 Manzilni kiriting.",
                reply_markup=cancel_keyboard(),
            )
            await state.set_state(AdminOrderStates.address)
            return
        await state.update_data(client_phone=phone)
        await message.answer("👤 Mijoz ismini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AdminOrderStates.name)

    @dp.message(AdminOrderStates.address)
    async def admin_order_address(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        address = (message.text or "").strip()
        if not address:
            await message.answer("⚠️ Manzilni kiriting.", reply_markup=cancel_keyboard())
            return
        products = db.list_products()
        if not products:
            await state.clear()
            await message.answer("📭 Mahsulotlar mavjud emas.", reply_markup=user_keyboard(message.from_user.id))
            return
        await state.update_data(address=address)
        await message.answer(
            "📦 Mahsulotni tanlang:",
            reply_markup=admin_order_products_keyboard(list(products)),
        )
        await state.set_state(AdminOrderStates.product)

    @dp.message(AdminOrderStates.product)
    async def admin_order_product_text(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        await message.answer("⚠️ Mahsulotni tanlash uchun tugmalardan foydalaning.")

    @dp.callback_query(F.data.startswith("admin_order_product:"))
    async def admin_order_product(callback: types.CallbackQuery, state: FSMContext) -> None:
        current_state = await state.get_state()
        if current_state != AdminOrderStates.product:
            await callback.answer()
            return
        product_id = int(callback.data.split(":", 1)[1])
        product = db.get_product(product_id)
        if not product:
            await callback.answer("❌ Mahsulot topilmadi", show_alert=True)
            return
        await state.update_data(product_id=product_id)
        if callback.message:
            await callback.message.answer(
                "⚖️ Buyurtma vaznini kiriting (masalan: 2.3 yoki 2,3 tonna).",
                reply_markup=cancel_keyboard(),
            )
        await state.set_state(AdminOrderStates.quantity)
        await callback.answer()

    @dp.message(AdminOrderStates.quantity)
    async def admin_order_quantity(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        qty_tons = parse_quantity_to_tons(message.text or "")
        if qty_tons is None or qty_tons <= 0:
            await message.answer(
                "⚠️ Miqdorni to'g'ri kiriting (masalan: 2, 2.3 yoki 2,3).",
                reply_markup=cancel_keyboard(),
            )
            return
        normalized_quantity = f"{format_price(qty_tons)} tonna"
        await state.update_data(quantity=normalized_quantity)
        await send_admin_order_confirmation(message, state)

    async def send_admin_order_confirmation(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        product = db.get_product(data["product_id"])
        if not product:
            await message.answer("❌ Mahsulot topilmadi.")
            await state.clear()
            return
        lines = [
            "🧾 Buyurtma ma'lumotlari:",
            f"👤 Mijoz: {escape(data['client_name'])}",
            f"📞 Telefon: {escape(data['client_phone'])}",
            f"📍 Manzil: {escape(data['address'])}",
            f"📦 Mahsulot: {escape(product['name'])}",
            f"⚖️ Miqdor: {escape(data['quantity'])}",
            f"💰 Narx (1 kg): {escape(format_price(product['price_per_kg']))} сум",
            f"💵 Jami: {escape(format_deal_price(data['quantity'], product['price_per_kg']))}",
            "",
            "Buyurtmani yaratishni tasdiqlaysizmi? Buyurtma «Yopilgan» holatida yaratiladi.",
        ]
        await message.answer(
            "\n".join(lines),
            reply_markup=admin_order_confirm_keyboard(),
            parse_mode="HTML",
        )
        await state.set_state(AdminOrderStates.confirm)

    @dp.callback_query(F.data == "admin_order_confirm")
    async def confirm_admin_order(callback: types.CallbackQuery, state: FSMContext) -> None:
        current_state = await state.get_state()
        if current_state != AdminOrderStates.confirm:
            await callback.answer()
            return
        data = await state.get_data()
        product = db.get_product(data["product_id"])
        if not product:
            await callback.answer("❌ Mahsulot topilmadi", show_alert=True)
            await state.clear()
            return
        user_id = data.get("user_id")
        if not user_id:
            user_id = db.add_manual_user(data["client_name"], data["client_phone"], callback.from_user.id)
        order_id = db.add_admin_order(
            user_id,
            product["id"],
            data["quantity"],
            data["address"],
            product["price_per_kg"],
            callback.from_user.id,
        )
        client_tg_id = data.get("client_tg_id")
        if client_tg_id and client_tg_id > 0:
            order_lines = [
                "🧾 Siz uchun buyurtma yaratildi:",
                f"🆔 Buyurtma ID: {order_id}",
                f"👤 Mijoz: {escape(data['client_name'])}",
                f"📞 Telefon: {escape(data['client_phone'])}",
                f"📍 Manzil: {escape(data['address'])}",
                f"📦 Mahsulot: {escape(product['name'])}",
                f"⚖️ Miqdor: {escape(data['quantity'])}",
                f"💰 Narx (1 kg): {escape(format_price(product['price_per_kg']))} сум",
                f"💵 Jami: {escape(format_deal_price(data['quantity'], product['price_per_kg']))}",
                "📌 Holat: Yopilgan",
            ]
            try:
                await bot.send_message(client_tg_id, "\n".join(order_lines), parse_mode="HTML")
            except Exception:
                pass
        await state.clear()
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(
                f"✅ Buyurtma yaratildi va yopildi. 🆔 ID: {order_id}",
                reply_markup=user_keyboard(callback.from_user.id),
            )
        await callback.answer("✅ Buyurtma yaratildi")

    @dp.callback_query(F.data == "admin_order_cancel")
    async def cancel_admin_order(callback: types.CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(
                "❌ Buyurtma bekor qilindi.", reply_markup=user_keyboard(callback.from_user.id)
            )
        await callback.answer("❌ Bekor qilindi")

    @dp.message(F.text == BTN_INFO)
    async def show_info(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(INFO_TEXT)

    @dp.message(F.text == BTN_CONTACT)
    async def show_contact(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(CONTACT_TEXT)

    @dp.message(F.text == BTN_NEWS)
    async def show_news(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(NEWS_TEXT, reply_markup=news_inline_keyboard())

    @dp.message(F.text == BTN_SUPPORT)
    async def support_start(message: types.Message, state: FSMContext) -> None:
        if not await ensure_user_registered(message):
            return
        await state.set_state(SupportStates.waiting_message)
        await message.answer(
            "🆘 Savolingizni yozing yoki rasm/video yuboring. "
            "Chiqish uchun Bekor qilish tugmasini bosing.",
            reply_markup=cancel_keyboard(),
        )

    @dp.message(SupportStates.waiting_message)
    async def support_receive(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer(
                "❌ Qo'llab-quvvatlash bekor qilindi.",
                reply_markup=user_keyboard(message.from_user.id),
            )
            return
        if message.media_group_id:
            reject_key = (message.from_user.id, message.media_group_id)
            if reject_key in support_media_group_reject:
                return
            support_media_group_reject.add(reject_key)
            await message.answer(
                "⚠️ Iltimos, faqat bitta rasm yuboring yoki faqat matn yuboring.",
                reply_markup=user_keyboard(message.from_user.id),
            )
            await state.clear()
            return
        if not GROUP_LIST:
            await message.answer(
                "⚠️ Hozircha qo'llab-quvvatlash guruhi mavjud emas.",
                reply_markup=user_keyboard(message.from_user.id),
            )
            await state.clear()
            return
        user = db.get_user_by_tg_id(message.from_user.id)
        phone = user["phone"] if user else None
        full_name = " ".join(
            part for part in [message.from_user.first_name, message.from_user.last_name] if part
        )
        name_display = full_name if full_name else "Noma'lum foydalanuvchi"
        support_text = format_support_user_details(
            name_display,
            phone,
            message.text or message.caption,
        )
        success = 0
        for group_id in GROUP_LIST:
            try:
                sent_message: Optional[types.Message] = None
                if message.photo:
                    sent_message = await message.bot.send_photo(
                        chat_id=group_id,
                        photo=message.photo[-1].file_id,
                        caption=support_text,
                    )
                elif message.video:
                    sent_message = await message.bot.send_video(
                        chat_id=group_id,
                        video=message.video.file_id,
                        caption=support_text,
                    )
                elif message.document:
                    sent_message = await message.bot.send_document(
                        chat_id=group_id,
                        document=message.document.file_id,
                        caption=support_text,
                    )
                else:
                    sent_message = await message.bot.send_message(
                        group_id,
                        support_text,
                    )
                if sent_message:
                    support_reply_map[(group_id, sent_message.message_id)] = message.from_user.id
                success += 1
            except Exception:
                continue
        if not success:
            await message.answer(
                "⚠️ Xabarni yuborib bo'lmadi. Iltimos, keyinroq urinib ko'ring.",
                reply_markup=user_keyboard(message.from_user.id),
            )
            await state.clear()
            return
        await message.answer(
            "✅ Xabaringiz yuborildi. Javobni shu yerda kuting.",
            reply_markup=user_keyboard(message.from_user.id),
        )
        await state.clear()

    @dp.message(F.text == BTN_BLOCK_USERS)
    async def block_users_menu(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await state.set_state(BlockUserStates.action)
        await message.answer(
            "🔐 Foydalanuvchini bloklash yoki blokdan chiqarishni tanlang.",
            reply_markup=block_action_keyboard(),
        )

    @dp.message(BlockUserStates.action)
    async def block_users_choose_action(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        action_text = (message.text or "").strip()
        if action_text not in {BTN_BLOCK, BTN_UNBLOCK}:
            await message.answer(
                "⚠️ Iltimos, bloklash yoki blokdan chiqarishni tanlang.",
                reply_markup=block_action_keyboard(),
            )
            return
        action_value = "block" if action_text == BTN_BLOCK else "unblock"
        await state.update_data(block_action=action_value)
        await message.answer(
            "📲 Telefon raqamini yuboring (masalan: +998901234567).",
            reply_markup=cancel_keyboard(),
        )
        await state.set_state(BlockUserStates.phone)

    @dp.message(BlockUserStates.phone)
    async def block_users_apply(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        user = find_user_by_phone(message.text or "")
        if not user:
            await message.answer(
                "⚠️ Foydalanuvchi topilmadi. Telefon raqamini tekshirib qayta yuboring.",
                reply_markup=cancel_keyboard(),
            )
            return
        if is_admin(user["tg_id"]):
            await message.answer(
                "⚠️ Admin foydalanuvchini bloklab bo'lmaydi.",
                reply_markup=user_keyboard(message.from_user.id),
            )
            await state.clear()
            return
        data = await state.get_data()
        action = data.get("block_action")
        name_display = format_user_contact(user["first_name"], user["last_name"], user["phone"])
        if action == "block":
            if user["is_blocked"]:
                await message.answer(
                    f"ℹ️ Foydalanuvchi allaqachon bloklangan: {name_display}.",
                    reply_markup=user_keyboard(message.from_user.id),
                )
            else:
                db.set_user_blocked(user["tg_id"], True)
                await message.answer(
                    f"✅ Foydalanuvchi bloklandi: {name_display}.",
                    reply_markup=user_keyboard(message.from_user.id),
                )
        else:
            if not user["is_blocked"]:
                await message.answer(
                    f"ℹ️ Foydalanuvchi bloklanmagan: {name_display}.",
                    reply_markup=user_keyboard(message.from_user.id),
                )
            else:
                db.set_user_blocked(user["tg_id"], False)
                await message.answer(
                    f"✅ Foydalanuvchi blokdan chiqarildi: {name_display}.",
                    reply_markup=user_keyboard(message.from_user.id),
                )
        await state.clear()

    @dp.message(F.text == BTN_STATS)
    async def show_stats(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        total = db.count_users()
        active = db.count_active_users(30)
        top_purchasers = db.list_top_purchasers()
        top_active_users = db.list_top_active_users()
        purchaser_lines = []
        for idx, row in enumerate(top_purchasers, start=1):
            contact = format_user_contact(row["first_name"], row["last_name"], row["phone"])
            purchaser_lines.append(f"{idx}. {contact} — {row['order_count']} ta")
        active_lines = []
        for idx, row in enumerate(top_active_users, start=1):
            contact = format_user_contact(row["first_name"], row["last_name"], row["phone"])
            active_lines.append(f"{idx}. {contact} — {row['activity_count']} ta")
        purchasers_text = "\n".join(purchaser_lines) if purchaser_lines else "Hozircha ma'lumot yo'q."
        active_users_text = "\n".join(active_lines) if active_lines else "Hozircha ma'lumot yo'q."
        await message.answer(
            "📊 Statistika:\n"
            f"👥 Umumiy foydalanuvchilar: {total}\n"
            f"🔥 So'nggi 30 kunda faol: {active}\n\n"
            "🏆 Ko'p marta buyurtma bergan foydalanuvchilar:\n"
            f"{purchasers_text}\n\n"
            "🚀 Botdan ko'p foydalanadigan foydalanuvchilar:\n"
            f"{active_users_text}"
        )

    @dp.message(F.text == BTN_REPORTS)
    async def report_start(message: types.Message, state: FSMContext) -> None:
        if not can_view_reports(message.from_user.id):
            return
        await state.set_state(ReportStates.start_date)
        await message.answer(
            "📅 Hisobot davrini tanlang yoki boshlanish sanasini kiriting "
            "(YYYY-MM-DD yoki DD.MM.YYYY).",
            reply_markup=report_period_keyboard(),
        )
        await message.answer(
            "✍️ Sana kiritish uchun: boshlanish sanasini yuboring.",
            reply_markup=cancel_keyboard(),
        )

    @dp.callback_query(F.data.startswith("report_period:"))
    async def report_quick_period(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not can_view_reports(callback.from_user.id):
            await callback.answer()
            return
        period_key = callback.data.split(":", 1)[1]
        now = datetime.now()
        if period_key == "current_month":
            start_date, end_date = get_month_range(now, 0)
        elif period_key == "previous_month":
            start_date, end_date = get_month_range(now, -1)
        elif period_key == "current_year":
            start_date, end_date = get_year_range(now.year)
        elif period_key == "previous_year":
            start_date, end_date = get_year_range(now.year - 1)
        else:
            await callback.answer("⚠️ Davr topilmadi.", show_alert=True)
            return
        await state.clear()
        if callback.message:
            await send_report_for_period(
                callback.message.bot,
                callback.message.chat.id,
                callback.from_user.id,
                start_date,
                end_date,
            )
        await callback.answer()

    @dp.message(ReportStates.start_date)
    async def report_start_date(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        start_date = parse_report_date(message.text or "")
        if not start_date:
            await message.answer(
                "⚠️ Sana formatini tekshiring (masalan: 2024-01-31 yoki 31.01.2024).",
                reply_markup=cancel_keyboard(),
            )
            return
        await state.update_data(report_start=start_date.strftime("%Y-%m-%d"))
        await state.set_state(ReportStates.end_date)
        await message.answer(
            "📅 Hisobot uchun tugash sanasini kiriting (YYYY-MM-DD yoki DD.MM.YYYY).",
            reply_markup=cancel_keyboard(),
        )

    @dp.message(ReportStates.end_date)
    async def report_end_date(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        end_date = parse_report_date(message.text or "")
        if not end_date:
            await message.answer(
                "⚠️ Sana formatini tekshiring (masalan: 2024-01-31 yoki 31.01.2024).",
                reply_markup=cancel_keyboard(),
            )
            return
        data = await state.get_data()
        start_date = datetime.strptime(data["report_start"], "%Y-%m-%d")
        if end_date < start_date:
            await message.answer(
                "⚠️ Tugash sanasi boshlanish sanasidan oldin bo'lmasligi kerak.",
                reply_markup=cancel_keyboard(),
            )
            return
        await send_report_for_period(
            message.bot,
            message.chat.id,
            message.from_user.id,
            start_date,
            end_date,
        )
        await state.clear()

    @dp.message(F.text == BTN_ORDERS_LIST)
    async def show_orders_summary(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        total = db.count_orders()
        open_count = db.count_orders_by_status("open")
        closed_count = db.count_orders_by_status("closed")
        canceled_count = db.count_orders_by_status("canceled")
        await message.answer(
            "🧾 Zayavkalar bo'yicha ma'lumot:\n"
            f"📦 Umumiy: {total}\n"
            f"✅ Yopilgan: {closed_count}\n"
            f"❌ Bekor qilingan: {canceled_count}\n"
            f"🟢 Ochiq: {open_count}",
            reply_markup=orders_status_keyboard(),
        )

    @dp.message(F.text == BTN_MY_ORDERS)
    async def show_user_orders(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        user = db.get_user_by_tg_id(message.from_user.id)
        if not user:
            await message.answer("❌ Foydalanuvchi topilmadi.")
            return
        orders = db.list_orders_for_user(user["id"])
        if not orders:
            await message.answer("📭 Sizda buyurtmalar mavjud emas.")
            return
        for order in orders:
            keyboard = None
            if order["status"] == "open":
                keyboard = user_order_action_keyboard(order["id"])
            await message.answer(
                format_user_order_message(order),
                reply_markup=keyboard,
                parse_mode="HTML",
            )

    @dp.callback_query(F.data.startswith("user_orders:cancel:"))
    async def prompt_user_cancel_order(callback: types.CallbackQuery) -> None:
        order_id = int(callback.data.split(":", 2)[2])
        if callback.message:
            await callback.message.edit_reply_markup(
                reply_markup=user_order_cancel_confirm_keyboard(order_id)
            )
        await callback.answer("❗ Buyurtmani bekor qilishni tasdiqlang")

    @dp.callback_query(F.data.startswith("user_orders:cancel_confirm:"))
    async def confirm_user_cancel_order(callback: types.CallbackQuery) -> None:
        order_id = int(callback.data.split(":", 3)[2])
        user = db.get_user_by_tg_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ Foydalanuvchi topilmadi.", show_alert=True)
            return
        updated, status, canceled_by_role = db.cancel_order_by_user(
            order_id,
            user["id"],
        )
        if not updated:
            if status == "closed":
                await callback.answer(
                    "✅ Buyurtma allaqachon qabul qilingan.", show_alert=True
                )
            elif status == "canceled" and canceled_by_role == "user":
                await callback.answer(
                    "❌ Buyurtma allaqachon bekor qilingan.", show_alert=True
                )
            elif status == "canceled":
                await callback.answer(
                    "❌ Buyurtma allaqachon bekor qilingan.", show_alert=True
                )
            else:
                await callback.answer("🔎 Buyurtma topilmadi.", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("❌ Buyurtma bekor qilindi")

    @dp.callback_query(F.data.startswith("user_orders:cancel_keep:"))
    async def cancel_user_cancel_order(callback: types.CallbackQuery) -> None:
        order_id = int(callback.data.split(":", 3)[2])
        if callback.message:
            await callback.message.edit_reply_markup(
                reply_markup=user_order_action_keyboard(order_id)
            )
        await callback.answer("↩️ Bekor qilinmadi")

    @dp.callback_query(F.data == "orders:open")
    async def show_open_orders(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        orders = db.list_orders_with_details(status="open")
        if not orders:
            await callback.message.answer("📭 Hozircha ochiq zayavkalar yo'q.")
            await callback.answer()
            return
        for order in orders:
            text = format_order_message(order)
            await callback.message.answer(
                text, reply_markup=order_action_keyboard(order["id"]), parse_mode="HTML"
            )
        await callback.answer()

    @dp.callback_query(F.data == "orders:search")
    async def prompt_order_search(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await state.set_state(OrderSearchStates.order_id)
        await callback.message.answer(
            "🔎 Buyurtma ID raqamini kiriting.",
            reply_markup=cancel_keyboard(),
        )
        await callback.answer()

    @dp.callback_query(F.data == "orders:delete")
    async def prompt_order_delete(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await state.set_state(OrderDeleteStates.order_id)
        await callback.message.answer(
            "🗑 O'chirish uchun buyurtma ID raqamini kiriting.",
            reply_markup=cancel_keyboard(),
        )
        await callback.answer()

    @dp.message(OrderDeleteStates.order_id)
    async def handle_order_delete_id(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer("❌ O'chirish bekor qilindi.", reply_markup=user_keyboard(message.from_user.id))
            return
        search_text = message.text or ""
        match = re.search(r"\d+", search_text)
        if not match:
            await message.answer(
                "⚠️ Iltimos, buyurtma ID raqamini kiriting.",
                reply_markup=cancel_keyboard(),
            )
            return
        order_id = int(match.group())
        order = db.get_order_with_details(order_id)
        if not order:
            await message.answer(
                "🔎 Buyurtma topilmadi. Qayta urinib ko'ring.",
                reply_markup=cancel_keyboard(),
            )
            return
        await state.update_data(order_id=order_id)
        confirmation_text = (
            "❗ Buyurtmani o'chirmoqchimisiz? Bu amal qaytarilmaydi.\n\n"
            f"{format_admin_order_details(order)}"
        )
        await message.answer(
            confirmation_text,
            reply_markup=order_delete_confirm_keyboard(),
            parse_mode="HTML",
        )
        await state.set_state(OrderDeleteStates.confirm)

    @dp.callback_query(OrderDeleteStates.confirm, F.data == "orders:delete_confirm")
    async def confirm_order_delete(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        data = await state.get_data()
        order_id = data.get("order_id")
        if not order_id:
            await state.clear()
            await callback.answer("⚠️ Buyurtma topilmadi.", show_alert=True)
            return
        removed = db.delete_order(order_id)
        await state.clear()
        if not removed:
            await callback.answer("🔎 Buyurtma topilmadi.", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(
                f"✅ Buyurtma o'chirildi. ID: {order_id}",
                reply_markup=user_keyboard(callback.from_user.id),
            )
        await callback.answer("✅ Buyurtma o'chirildi")

    @dp.callback_query(OrderDeleteStates.confirm, F.data == "orders:delete_keep")
    async def cancel_order_delete(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await state.clear()
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(
                "↩️ Buyurtmani o'chirish bekor qilindi.",
                reply_markup=user_keyboard(callback.from_user.id),
            )
        await callback.answer("↩️ Bekor qilindi")

    @dp.message(OrderSearchStates.order_id)
    async def handle_order_search(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer("❌ Qidiruv bekor qilindi.", reply_markup=user_keyboard(message.from_user.id))
            return
        search_text = message.text or ""
        match = re.search(r"\d+", search_text)
        if not match:
            await message.answer(
                "⚠️ Iltimos, buyurtma ID raqamini kiriting.",
                reply_markup=cancel_keyboard(),
            )
            return
        order_id = int(match.group())
        order = db.get_order_with_details(order_id)
        if not order:
            await message.answer(
                "🔎 Buyurtma topilmadi.",
                reply_markup=cancel_keyboard(),
            )
            return
        keyboard = order_action_keyboard(order_id) if order["status"] == "open" else None
        await message.answer(
            format_admin_order_details(order),
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await message.answer(
            "🔁 Yana bir ID kiriting yoki Bekor qilish tugmasini bosing.",
            reply_markup=cancel_keyboard(),
        )

    @dp.callback_query(F.data.startswith("orders:close:"))
    async def close_order_status(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        order_id = int(callback.data.split(":", 2)[2])
        updated, status, closed_by, canceled_by_role = db.update_order_status(
            order_id, "closed", callback.from_user.id
        )
        if not updated:
            if status == "canceled":
                if canceled_by_role == "user":
                    await callback.answer(
                        "❌ Mijoz buyurtmani bekor qilgan.", show_alert=True
                    )
                else:
                    await callback.answer("❌ Status allaqachon bekor qilingan.", show_alert=True)
            elif closed_by and closed_by != callback.from_user.id:
                await callback.answer("⚠️ Boshqa admin allaqachon statusni yopgan.", show_alert=True)
            else:
                await callback.answer("✅ Status allaqachon yopilgan.", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("✅ Zayavka qabul qilindi va yopildi")

    @dp.callback_query(F.data.startswith("orders:cancel:"))
    async def prompt_cancel_order(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        order_id = int(callback.data.split(":", 2)[2])
        if callback.message:
            await callback.message.edit_reply_markup(
                reply_markup=order_cancel_confirm_keyboard(order_id)
            )
        await callback.answer("❗ Zayavkani bekor qilishni tasdiqlang", show_alert=False)

    @dp.callback_query(F.data.startswith("orders:cancel_confirm:"))
    async def cancel_order_status(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        order_id = int(callback.data.split(":", 3)[2])
        updated, status, closed_by, canceled_by_role = db.update_order_status(
            order_id, "canceled", callback.from_user.id
        )
        if not updated:
            if status == "closed":
                await callback.answer("✅ Status allaqachon yopilgan.", show_alert=True)
            elif status == "canceled" and canceled_by_role == "user":
                await callback.answer("❌ Mijoz buyurtmani bekor qilgan.", show_alert=True)
            elif closed_by and closed_by != callback.from_user.id:
                await callback.answer("⚠️ Boshqa admin allaqachon bekor qilgan.", show_alert=True)
            else:
                await callback.answer("❌ Status allaqachon bekor qilingan.", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("❌ Zayavka bekor qilindi")

    @dp.callback_query(F.data.startswith("orders:cancel_keep:"))
    async def cancel_order_keep(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        order_id = int(callback.data.split(":", 3)[2])
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=order_action_keyboard(order_id))
        await callback.answer("↩️ Bekor qilinmadi")

    @dp.callback_query(F.data.startswith("orders:closed:"))
    async def show_closed_orders(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        offset = int(callback.data.split(":", 2)[2])
        limit = 10
        orders = db.list_orders_with_details(status="closed", limit=limit, offset=offset)
        if not orders:
            if offset == 0:
                await callback.message.answer("📭 Yopilgan zayavkalar yo'q.")
            else:
                await callback.message.answer("📭 Boshqa yopilgan zayavkalar yo'q.")
            await callback.answer()
            return
        lines = []
        for idx, order in enumerate(orders, start=offset + 1):
            lines.append(
                "\n".join(
                    [
                        f"{idx}. {format_order_message(order, include_id=True, include_address=True)}"
                    ]
                )
            )
        message_text = "\n\n".join(lines)
        total_closed = db.count_orders_by_status("closed")
        keyboard = None
        if offset + limit < total_closed:
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="➡️ Yana 10 ta", callback_data=f"orders:closed:{offset + limit}"
                        )
                    ]
                ]
            )
        await callback.message.answer(message_text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()

    @dp.callback_query(F.data.startswith("orders:canceled:"))
    async def show_canceled_orders(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        offset = int(callback.data.split(":", 2)[2])
        limit = 10
        orders = db.list_orders_with_details(status="canceled", limit=limit, offset=offset)
        if not orders:
            if offset == 0:
                await callback.message.answer("📭 Bekor qilingan zayavkalar yo'q.")
            else:
                await callback.message.answer("📭 Boshqa bekor qilingan zayavkalar yo'q.")
            await callback.answer()
            return
        lines = []
        for idx, order in enumerate(orders, start=offset + 1):
            lines.append(
                "\n".join(
                    [
                        f"{idx}. {format_order_message(order, include_id=True, include_address=True)}"
                    ]
                )
            )
        message_text = "\n\n".join(lines)
        total_canceled = db.count_orders_by_status("canceled")
        keyboard = None
        if offset + limit < total_canceled:
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="➡️ Yana 10 ta", callback_data=f"orders:canceled:{offset + limit}"
                        )
                    ]
                ]
            )
        await callback.message.answer(message_text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()

    @dp.message(F.text == BTN_ADD_PRODUCT)
    async def add_product_start(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await message.answer("📝 Mahsulot nomini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AddProductStates.name)

    @dp.callback_query(F.data == "add_product")
    async def add_product_inline(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await callback.message.answer("📝 Mahsulot nomini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AddProductStates.name)
        await callback.answer()

    @dp.message(AddProductStates.name)
    async def add_product_name(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        await state.update_data(name=message.text)
        await message.answer("💰 Narxini kiriting (1 kg uchun).", reply_markup=cancel_keyboard())
        await state.set_state(AddProductStates.price)

    @dp.message(AddProductStates.price)
    async def add_product_price(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        price = parse_price(message.text)
        if price is None:
            await message.answer("⚠️ Narxni to'g'ri kiriting (masalan: 12000).")
            return
        await state.update_data(price=price)
        await message.answer(
            "🗒 Tavsifini kiriting yoki o'tkazib yuboring.",
            reply_markup=description_keyboard(),
        )
        await state.set_state(AddProductStates.description)

    @dp.message(AddProductStates.description)
    async def add_product_description(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        if message.text and message.text.strip() == BTN_SKIP_DESCRIPTION:
            await state.update_data(description=None)
        else:
            await state.update_data(description=message.text)
        await message.answer(
            "🖼 Agar rasm bo'lsa yuboring (1 dona). O'tkazish uchun 'O'tkazish' tugmasini bosing.",
            reply_markup=add_product_photos_keyboard(),
        )
        await state.set_state(AddProductStates.photos)

    @dp.message(AddProductStates.photos)
    async def add_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        if message.text and message.text.strip() == BTN_SKIP_PHOTOS:
            product_id = db.add_product(data["name"], data["price"], data["description"])
            if photos:
                db.set_product_photos(product_id, photos[:1])
            await message.answer("✅ Mahsulot qo'shildi.", reply_markup=user_keyboard(message.from_user.id))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "📷 Iltimos, rasm yuboring yoki 'O'tkazish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        product_id = db.add_product(data["name"], data["price"], data["description"])
        db.set_product_photos(product_id, photos[:1])
        await message.answer("✅ Mahsulot qo'shildi.", reply_markup=user_keyboard(message.from_user.id))
        await state.clear()

    @dp.message(F.text == BTN_EDIT_PRODUCT)
    async def edit_product_list(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        products = db.list_products()
        if not products:
            await message.answer("📭 Mahsulotlar mavjud emas.")
            return
        for product in products:
            await message.answer(
                f"{product['name']} (ID: {product['id']})",
                reply_markup=edit_inline_keyboard(product["id"]),
            )

    @dp.callback_query(F.data.startswith("edit:"))
    async def edit_product_start(callback: types.CallbackQuery, state: FSMContext) -> None:
        product_id = int(callback.data.split(":", 1)[1])
        await state.update_data(product_id=product_id)
        await callback.message.answer("✏️ Nimani tahrirlaysiz?", reply_markup=edit_fields_keyboard())
        await callback.message.answer(
            "❌ Agar bekor qilmoqchi bo'lsangiz, Bekor qilish tugmasini bosing.",
            reply_markup=cancel_keyboard(),
        )
        await state.set_state(EditProductStates.field)
        await callback.answer()

    @dp.message(EditProductStates.field)
    async def edit_product_cancel(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)

    @dp.callback_query(EditProductStates.field, F.data.startswith("field:"))
    async def edit_product_field(callback: types.CallbackQuery, state: FSMContext) -> None:
        field = callback.data.split(":", 1)[1]
        if field == "delete":
            data = await state.get_data()
            product_id = data["product_id"]
            await callback.message.answer(
                "🗑 Mahsulotni o'chirishni tasdiqlaysizmi?",
                reply_markup=delete_product_confirm_keyboard(product_id),
            )
            await callback.answer()
            return
        await state.update_data(field=field)
        if field == "photos":
            await callback.message.answer(
                "🖼 Yangi rasmni yuboring (1 dona). Tugatish: 'O'tkazish' tugmasi.",
                reply_markup=add_product_photos_keyboard(),
            )
            await state.set_state(EditProductStates.photos)
        else:
            await callback.message.answer("📝 Yangi qiymatni kiriting.", reply_markup=cancel_keyboard())
            await state.set_state(EditProductStates.value)
        await callback.answer()

    @dp.message(EditProductStates.value)
    async def edit_product_value(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        product_id = data["product_id"]
        field = data["field"]
        if field == "name":
            db.update_product_name(product_id, message.text)
        elif field == "price":
            price = parse_price(message.text)
            if price is None:
                await message.answer("⚠️ Narxni to'g'ri kiriting.")
                return
            db.update_product_price(product_id, price)
        elif field == "description":
            db.update_product_description(product_id, message.text)
        await message.answer("✅ Mahsulot yangilandi.", reply_markup=user_keyboard(message.from_user.id))
        await state.clear()

    @dp.message(EditProductStates.photos)
    async def edit_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        if message.text and message.text.strip() == BTN_SKIP_PHOTOS:
            if photos:
                db.set_product_photos(data["product_id"], photos[:1])
            else:
                db.set_product_photos(data["product_id"], [])
            await message.answer("✅ Rasmlar yangilandi.", reply_markup=user_keyboard(message.from_user.id))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "📷 Iltimos, rasm yuboring yoki 'O'tkazish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        db.set_product_photos(data["product_id"], photos[:1])
        await message.answer("✅ Rasmlar yangilandi.", reply_markup=user_keyboard(message.from_user.id))
        await state.clear()

    @dp.callback_query(F.data.startswith("product_delete:confirm:"))
    async def confirm_product_delete(
        callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        product_id = int(callback.data.split(":", 2)[2])
        removed = db.delete_product(product_id)
        if not removed:
            await callback.answer("🔎 Mahsulot topilmadi.", show_alert=True)
            return
        await state.clear()
        await callback.message.answer("🗑 Mahsulot o'chirildi.", reply_markup=user_keyboard(callback.from_user.id))
        await callback.answer()

    @dp.callback_query(F.data.startswith("product_delete:cancel:"))
    async def cancel_product_delete(
        callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        product_id = int(callback.data.split(":", 2)[2])
        await state.update_data(product_id=product_id)
        await state.set_state(EditProductStates.field)
        await callback.message.answer("✏️ Nimani tahrirlaysiz?", reply_markup=edit_fields_keyboard())
        await callback.answer("↩️ O'chirish bekor qilindi")

    @dp.message(F.text == BTN_BROADCAST)
    async def broadcast_start(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await message.answer("📣 Tarqatma uchun matn, foto yoki video yuboring.")
        await state.set_state(BroadcastStates.content)

    @dp.message(BroadcastStates.content)
    async def broadcast_content(message: types.Message, state: FSMContext) -> None:
        if message.media_group_id:
            buffer_entry = media_group_buffer.setdefault(
                message.from_user.id,
                {"media_items": [], "caption": None, "finalized": False},
            )
            if message.photo:
                buffer_entry["media_items"].append(
                    {"type": "photo", "file_id": message.photo[-1].file_id}
                )
            elif message.video:
                buffer_entry["media_items"].append(
                    {"type": "video", "file_id": message.video.file_id}
                )
            if not buffer_entry.get("caption") and safe_caption(message):
                buffer_entry["caption"] = safe_caption(message)
            if not buffer_entry.get("timer"):
                buffer_entry["timer"] = asyncio.create_task(
                    handle_media_group_timeout(message.from_user.id, message.bot, state)
                )
            return

        if message.photo:
            payload = BroadcastPayload(
                kind="photo",
                file_ids=[message.photo[-1].file_id],
                caption=safe_caption(message),
            )
        elif message.video:
            payload = BroadcastPayload(
                kind="video",
                file_ids=[message.video.file_id],
                caption=safe_caption(message),
            )
        else:
            payload = BroadcastPayload(kind="text", text=message.text)

        await state.update_data(broadcast_payload=payload)
        await message.answer("📣 Tarqatmani tasdiqlaysizmi? (Ha/Yo'q)")
        await state.set_state(BroadcastStates.confirm)

    @dp.message(BroadcastStates.confirm)
    async def broadcast_confirm(message: types.Message, state: FSMContext) -> None:
        text = message.text.lower() if message.text else ""
        if text not in {"ha", "yo'q", "yoq"}:
            await message.answer("⚠️ Iltimos, Ha yoki Yo'q deb javob bering.")
            return
        if text in {"yo'q", "yoq"}:
            await message.answer("❌ Tarqatma bekor qilindi.")
            await state.clear()
            return

        data = await state.get_data()
        payload: BroadcastPayload = data["broadcast_payload"]
        users = db.list_users()
        success = 0
        failed = 0
        for user in users:
            try:
                if payload.kind == "text":
                    await bot.send_message(user["tg_id"], payload.text or "")
                elif payload.kind == "photo":
                    await bot.send_photo(
                        user["tg_id"], payload.file_ids[0], caption=payload.caption
                    )
                elif payload.kind == "video":
                    await bot.send_video(
                        user["tg_id"], payload.file_ids[0], caption=payload.caption
                    )
                elif payload.kind == "media_group":
                    builder = MediaGroupBuilder(caption=payload.caption)
                    for item in payload.media_items[:10]:
                        if item["type"] == "video":
                            builder.add_video(media=item["file_id"])
                        else:
                            builder.add_photo(media=item["file_id"])
                    await bot.send_media_group(user["tg_id"], media=builder.build())
                success += 1
            except Exception:
                failed += 1
        await message.answer(
            f"✅ Tarqatma yakunlandi. Muvaffaqiyatli: {success}, Xatolar: {failed}."
        )
        await state.clear()

    @dp.message(F.reply_to_message)
    async def support_admin_reply(message: types.Message) -> None:
        if message.chat.id not in GROUP_LIST or not is_admin(message.from_user.id):
            return
        if message.media_group_id:
            reject_key = (message.chat.id, message.media_group_id)
            if reject_key in admin_media_group_reject:
                return
            admin_media_group_reject.add(reject_key)
            await message.answer(
                "⚠️ Iltimos, faqat bitta rasm yuboring yoki faqat matn yuboring.",
            )
            return
        reply_to = message.reply_to_message
        if not reply_to:
            return
        user_id = support_reply_map.get((message.chat.id, reply_to.message_id))
        if not user_id:
            user_id = parse_support_user_id(reply_to.text)
        if not user_id:
            return
        await message.bot.send_message(user_id, "💬 Qo'llab-quvvatlashdan javob:")
        await message.bot.copy_message(
            chat_id=user_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )

    @dp.message()
    async def fallback(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer("👉 Iltimos, menyudan tanlang.")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
