import asyncio
import json
import logging
import os
import re
import urllib.parse
import urllib.request
from html import escape
from datetime import datetime
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

ADMIN_LIST = {960217500, 7746040125, 8359092913}
GROUP_LIST = {-1004745573608,}

INFO_TEXT = """ℹ️ Bizning botda mahsulotlar haqida ma'lumot olishingiz mumkin.
⚖️ Mahsulotlar narxi kilogramm bo'yicha ko'rsatiladi.
"""
CONTACT_TEXT = """📞 Aloqa uchun:
☎️ Telefon: +998 90 000 00 00
📍 Manzil: Toshkent shahar
"""
NEWS_TEXT = """📰 Yangiliklar hozircha mavjud emas."""

BTN_PRODUCTS = "📦 Mahsulotlar"
BTN_MY_ORDERS = "📦 Mening buyurtmalarim"
BTN_INFO = "ℹ️ Ma'lumot"
BTN_CONTACT = "📞 Aloqa"
BTN_NEWS = "📰 Yangiliklar"
BTN_STATS = "📊 Statistika"
BTN_ORDERS_LIST = "🧾 Buyurtmalar ro'yxati"
BTN_BROADCAST = "📣 Xabar tarqatish"
BTN_ADD_PRODUCT = "➕ Mahsulot qo'shish"
BTN_EDIT_PRODUCT = "✏️ Mahsulotni tahrirlash"
BTN_SEND_PHONE = "📲 Telefon raqamni yuborish"
BTN_FINISH = "✅ Tugatish"
BTN_CANCEL = "❌ Bekor qilish"
BTN_SEND_LOCATION = "📍 Lokatsiyani yuborish"
BTN_SUPPORT = "🆘 Qo'llab-quvvatlash"


class OrderStates(StatesGroup):
    quantity = State()
    address = State()
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


class SupportStates(StatesGroup):
    waiting_message = State()


@dataclass
class BroadcastPayload:
    kind: str
    text: Optional[str] = None
    file_ids: Optional[list[str]] = None
    caption: Optional[str] = None
    media_items: Optional[list[dict[str, str]]] = None


media_group_buffer: dict[int, dict[str, object]] = {}
support_reply_map: dict[tuple[int, int], int] = {}


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


def user_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_PRODUCTS)],
        [KeyboardButton(text=BTN_INFO), KeyboardButton(text=BTN_CONTACT)],
        [KeyboardButton(text=BTN_NEWS)],
    ]
    if not is_admin:
        rows.insert(1, [KeyboardButton(text=BTN_MY_ORDERS)])
        rows.append([KeyboardButton(text=BTN_SUPPORT)])
    if is_admin:
        rows.append([KeyboardButton(text=BTN_STATS), KeyboardButton(text=BTN_ORDERS_LIST)])
        rows.append([KeyboardButton(text=BTN_BROADCAST)])
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
            [KeyboardButton(text=BTN_FINISH), KeyboardButton(text=BTN_PRODUCTS)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
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


def format_user_contact(first_name: Optional[str], last_name: Optional[str], phone: Optional[str]) -> str:
    name_parts = [part for part in [first_name, last_name] if part]
    full_name = " ".join(name_parts) if name_parts else "Noma'lum foydalanuvchi"
    phone_display = phone or "📞 Telefon yo'q"
    return f"{full_name} ({phone_display})"


async def cancel_admin_action(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Amal bekor qilindi.", reply_markup=user_keyboard(True))


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
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
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


def format_support_user_details(user: types.User) -> str:
    username = f"@{user.username}" if user.username else "username yo'q"
    full_name = " ".join(part for part in [user.first_name, user.last_name] if part)
    name_display = full_name if full_name else "Noma'lum foydalanuvchi"
    return (
        "🆘 Yangi qo'llab-quvvatlash so'rovi\n"
        f"👤 Foydalanuvchi: {name_display}\n"
        f"🔗 Username: {username}\n"
        f"🆔 ID: {user.id}\n"
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
        return await asyncio.to_thread(_lookup)
    except Exception:
        return None


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    token = "8396669139:AAFvr8gWi7uXDMwPLBePF9NmYf16wsHmtPU"
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    db.init_db()


    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())
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
                "👋 Xush kelibsiz!", reply_markup=user_keyboard(is_admin(message.from_user.id))
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
            reply_markup=user_keyboard(is_admin(message.from_user.id)),
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
                "❌ Ariza bekor qilindi.", reply_markup=user_keyboard(is_admin(message.from_user.id))
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
            "✅ Buyurtma tasdiqlandi!", reply_markup=user_keyboard(is_admin(message.from_user.id))
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
                "❌ Ariza bekor qilindi.", reply_markup=user_keyboard(is_admin(message.from_user.id))
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
                "❌ Buyurtma bekor qilindi.", reply_markup=user_keyboard(is_admin(callback.from_user.id))
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
        await message.answer(NEWS_TEXT)

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
                reply_markup=user_keyboard(is_admin(message.from_user.id)),
            )
            return
        if not GROUP_LIST:
            await message.answer(
                "⚠️ Hozircha qo'llab-quvvatlash guruhi mavjud emas.",
                reply_markup=user_keyboard(is_admin(message.from_user.id)),
            )
            await state.clear()
            return
        success = 0
        for group_id in GROUP_LIST:
            try:
                detail_message = await message.bot.send_message(
                    group_id,
                    format_support_user_details(message.from_user),
                )
                forwarded = await message.bot.copy_message(
                    chat_id=group_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                )
                support_reply_map[(group_id, detail_message.message_id)] = message.from_user.id
                support_reply_map[(group_id, forwarded.message_id)] = message.from_user.id
                success += 1
            except Exception:
                continue
        if not success:
            await message.answer(
                "⚠️ Xabarni yuborib bo'lmadi. Iltimos, keyinroq urinib ko'ring.",
                reply_markup=user_keyboard(is_admin(message.from_user.id)),
            )
            await state.clear()
            return
        await message.answer(
            "✅ Xabaringiz yuborildi. Javobni shu yerda kuting.",
            reply_markup=user_keyboard(is_admin(message.from_user.id)),
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

    @dp.message(OrderSearchStates.order_id)
    async def handle_order_search(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await state.clear()
            await message.answer("❌ Qidiruv bekor qilindi.", reply_markup=user_keyboard(True))
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
        await message.answer("🗒 Tavsifini kiriting.", reply_markup=cancel_keyboard())
        await state.set_state(AddProductStates.description)

    @dp.message(AddProductStates.description)
    async def add_product_description(message: types.Message, state: FSMContext) -> None:
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        await state.update_data(description=message.text)
        await message.answer(
            "🖼 Agar rasm bo'lsa yuboring (1 dona). Tugatish uchun 'Tugatish' tugmasini bosing.",
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
        if message.text and message.text.strip() == BTN_PRODUCTS:
            await state.clear()
            await show_products(message)
            return
        if message.text and message.text.strip() == BTN_FINISH:
            product_id = db.add_product(data["name"], data["price"], data["description"])
            if photos:
                db.set_product_photos(product_id, photos[:1])
            await message.answer("✅ Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "📷 Iltimos, rasm yuboring yoki 'Tugatish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        product_id = db.add_product(data["name"], data["price"], data["description"])
        db.set_product_photos(product_id, photos[:1])
        await message.answer("✅ Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
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
                "🖼 Yangi rasmni yuboring (1 dona). Tugatish: 'Tugatish' tugmasi.",
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
        await message.answer("✅ Mahsulot yangilandi.", reply_markup=user_keyboard(True))
        await state.clear()

    @dp.message(EditProductStates.photos)
    async def edit_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if is_cancel_message(message):
            await cancel_admin_action(message, state)
            return
        if message.text and message.text.strip() == BTN_PRODUCTS:
            db.set_product_photos(data["product_id"], [])
            await state.clear()
            await show_products(message)
            return
        if message.text and message.text.strip() == BTN_FINISH:
            if photos:
                db.set_product_photos(data["product_id"], photos[:1])
            else:
                db.set_product_photos(data["product_id"], [])
            await message.answer("✅ Rasmlar yangilandi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "📷 Iltimos, rasm yuboring yoki 'Tugatish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        db.set_product_photos(data["product_id"], photos[:1])
        await message.answer("✅ Rasmlar yangilandi.", reply_markup=user_keyboard(True))
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
        await callback.message.answer("🗑 Mahsulot o'chirildi.", reply_markup=user_keyboard(True))
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