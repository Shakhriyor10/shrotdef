import asyncio
import logging
import os
import re
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

ADMIN_LIST = {960217500, 7746040125}

INFO_TEXT = """Bizning botda mahsulotlar haqida ma'lumot olishingiz mumkin.
Mahsulotlar narxi kilogramm bo'yicha ko'rsatiladi.
"""
CONTACT_TEXT = """Aloqa uchun:
Telefon: +998 90 000 00 00
Manzil: Toshkent shahar
"""
NEWS_TEXT = """Yangiliklar hozircha mavjud emas."""


class OrderStates(StatesGroup):
    quantity = State()
    address = State()


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


@dataclass
class BroadcastPayload:
    kind: str
    text: Optional[str] = None
    file_ids: Optional[list[str]] = None
    caption: Optional[str] = None
    media_items: Optional[list[dict[str, str]]] = None


media_group_buffer: dict[int, dict[str, object]] = {}


class ActivityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, types.Message) and event.from_user:
            db.update_last_active(event.from_user.id)
        return await handler(event, data)


def user_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Mahsulotlar")],
        [KeyboardButton(text="Ma'lumot"), KeyboardButton(text="Aloqa")],
        [KeyboardButton(text="Yangiliklar")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="Statistika"), KeyboardButton(text="Список заявки")])
        rows.append([KeyboardButton(text="Рассылка")])
        rows.append([
            KeyboardButton(text="Mahsulot qo'shish"),
            KeyboardButton(text="Mahsulotni tahrirlash"),
        ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Telefon raqamni yuborish", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def add_product_photos_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Tugatish"), KeyboardButton(text="Mahsulotlar")]],
        resize_keyboard=True,
    )


def product_inline_keyboard(product_id: int, admin: bool) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="Sotib olish uchun ariza yuborish", callback_data=f"order:{product_id}")]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="Tahrirlash", callback_data=f"edit:{product_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def edit_inline_keyboard(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Tahrirlash", callback_data=f"edit:{product_id}")]]
    )


def edit_fields_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Nomi", callback_data="field:name")],
            [InlineKeyboardButton(text="Narxi", callback_data="field:price")],
            [InlineKeyboardButton(text="Tavsif", callback_data="field:description")],
            [InlineKeyboardButton(text="Rasmlar", callback_data="field:photos")],
        ]
    )


def orders_status_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Не закрытые статусы", callback_data="orders:open"),
                InlineKeyboardButton(text="Закрытые статусы", callback_data="orders:closed:0"),
            ]
        ]
    )


def order_close_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Закрыть статус", callback_data=f"orders:close:{order_id}")]
        ]
    )


def format_order_message(order, include_id: bool = True, include_address: bool = True) -> str:
    person = format_order_person(order["first_name"], order["last_name"])
    created_at = format_order_datetime(order["created_at"])
    price_per_kg = order["order_price_per_kg"] or order["product_price_per_kg"]
    lines = []
    if include_id:
        lines.append(f"ID: {order['id']}")
    lines.extend(
        [
            f"Ism: {person}",
            f"Mahsulot: {order['product_name']}",
            f"Miqdor: {order['quantity']}",
            f"Narx (1 kg, ariza vaqti): {format_price(price_per_kg)}",
            f"Telefon: {order['phone'] or 'Kiritilmagan'}",
        ]
    )
    if include_address:
        lines.append(f"Manzil: {order['address']}")
    lines.append(f"Sana: {created_at}")
    return "\n".join(lines)


async def notify_admins_new_order(bot: Bot, order_id: int) -> None:
    order = db.get_order_with_details(order_id)
    if not order:
        return
    text = "Yangi ariza:\n" + format_order_message(order)
    for admin_id in ADMIN_LIST:
        try:
            await bot.send_message(
                admin_id,
                text,
                reply_markup=order_close_keyboard(order_id),
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
    return " ".join(parts) if parts else "Noma'lum"


def format_price(value: Optional[float]) -> str:
    if value is None:
        return "Kiritilmagan"
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def parse_quantity_to_kg(value: str) -> Optional[float]:
    cleaned = value.strip().lower()
    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", cleaned)
    if not match:
        return None
    number = float(match.group(1).replace(",", "."))
    if any(unit in cleaned for unit in ["tonna", "тонна", "t"]):
        return number * 1000
    return number


def format_deal_price(quantity: str, price_per_kg: Optional[float]) -> str:
    if price_per_kg is None:
        return "Hisoblab bo'lmadi"
    qty_kg = parse_quantity_to_kg(quantity)
    if qty_kg is None:
        return "Hisoblab bo'lmadi"
    return format_price(qty_kg * price_per_kg)


async def send_product(chat_id: int, product, bot: Bot, admin: bool) -> None:
    photos = db.get_product_photos(product["id"])
    caption = (
        f"Mahsulot: {product['name']}\n"
        f"Narxi (1 kg): {product['price_per_kg']}\n"
        f"Tavsif: {product['description'] or 'Kiritilmagan'}"
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
            "Iltimos, botdan foydalanish uchun telefon raqamingizni yuboring.",
            reply_markup=contact_keyboard(),
        )
        return False
    return True


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
    await bot.send_message(user_id, "Рассылkani tasdiqlaysizmi? (Ha/Yo'q)")
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
                "Xush kelibsiz!", reply_markup=user_keyboard(is_admin(message.from_user.id))
            )
        else:
            await message.answer(
                "Assalomu alaykum! Botdan foydalanish uchun telefon raqamingizni yuboring.",
                reply_markup=contact_keyboard(),
            )

    @dp.message(F.contact)
    async def handle_contact(message: types.Message) -> None:
        if not message.contact or message.contact.user_id != message.from_user.id:
            await message.answer("Iltimos, o'zingizning raqamingizni yuboring.")
            return
        db.update_user_phone(message.from_user.id, message.contact.phone_number)
        await message.answer(
            "Rahmat! Endi botdan foydalanishingiz mumkin.",
            reply_markup=user_keyboard(is_admin(message.from_user.id)),
        )

    @dp.message(F.text == "Mahsulotlar")
    async def show_products(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        admin = is_admin(message.from_user.id)
        products = db.list_products()
        if not products:
            await message.answer("Hozircha mahsulotlar mavjud emas.")
            if admin:
                await message.answer(
                    "Mahsulot qo'shish uchun pastdagi tugmani bosing.",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="Mahsulot qo'shish", callback_data="add_product")]
                        ]
                    ),
                )
            return
        for product in products:
            await send_product(message.chat.id, product, bot, admin)
        if admin:
            await message.answer(
                "Mahsulot qo'shish uchun pastdagi tugmani bosing.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Mahsulot qo'shish", callback_data="add_product")]
                    ]
                ),
            )

    @dp.callback_query(F.data.startswith("order:"))
    async def order_start(callback: types.CallbackQuery, state: FSMContext) -> None:
        product_id = int(callback.data.split(":", 1)[1])
        await state.update_data(product_id=product_id)
        await callback.message.answer(
            "Necha kg yoki necha tonna kerak? (masalan: 150 kg yoki 2 tonna)"
        )
        await state.set_state(OrderStates.quantity)
        await callback.answer()

    @dp.message(OrderStates.quantity)
    async def order_quantity(message: types.Message, state: FSMContext) -> None:
        await state.update_data(quantity=message.text)
        await message.answer("Yetkazib berish manzilini kiriting.")
        await state.set_state(OrderStates.address)

    @dp.message(OrderStates.address)
    async def order_address(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        user = db.get_user_by_tg_id(message.from_user.id)
        if not user:
            await message.answer("Foydalanuvchi topilmadi.")
            await state.clear()
            return
        product = db.get_product(data["product_id"])
        if not product:
            await message.answer("Mahsulot topilmadi.")
            await state.clear()
            return
        order_id = db.add_order(
            user["id"],
            data["product_id"],
            data["quantity"],
            message.text,
            product["price_per_kg"],
        )
        await message.answer("Arizangiz qabul qilindi!", reply_markup=user_keyboard(is_admin(message.from_user.id)))
        await notify_admins_new_order(message.bot, order_id)
        await state.clear()

    @dp.message(F.text == "Ma'lumot")
    async def show_info(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(INFO_TEXT)

    @dp.message(F.text == "Aloqa")
    async def show_contact(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(CONTACT_TEXT)

    @dp.message(F.text == "Yangiliklar")
    async def show_news(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer(NEWS_TEXT)

    @dp.message(F.text == "Statistika")
    async def show_stats(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        total = db.count_users()
        active = db.count_active_users(30)
        await message.answer(
            f"Umumiy foydalanuvchilar: {total}\nSo'nggi 30 kunda faol: {active}"
        )

    @dp.message(F.text == "Список заявки")
    async def show_orders_summary(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        total = db.count_orders()
        open_count = db.count_orders_by_status("open")
        closed_count = db.count_orders_by_status("closed")
        await message.answer(
            "Zayavkalar bo'yicha ma'lumot:\n"
            f"Umumiy: {total}\n"
            f"Yopilgan: {closed_count}\n"
            f"Ochiq: {open_count}",
            reply_markup=orders_status_keyboard(),
        )

    @dp.callback_query(F.data == "orders:open")
    async def show_open_orders(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        orders = db.list_orders_with_details(status="open")
        if not orders:
            await callback.message.answer("Hozircha ochiq zayavkalar yo'q.")
            await callback.answer()
            return
        for order in orders:
            text = format_order_message(order)
            await callback.message.answer(
                text, reply_markup=order_close_keyboard(order["id"])
            )
        await callback.answer()

    @dp.callback_query(F.data.startswith("orders:close:"))
    async def close_order_status(callback: types.CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        order_id = int(callback.data.split(":", 2)[2])
        updated, closed_by = db.close_order(order_id, callback.from_user.id)
        if not updated:
            if closed_by and closed_by != callback.from_user.id:
                await callback.answer("Boshqa admin allaqachon statusni yopgan.", show_alert=True)
            else:
                await callback.answer("Status allaqachon yopilgan.", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer("Status yopildi")

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
                await callback.message.answer("Yopilgan zayavkalar yo'q.")
            else:
                await callback.message.answer("Boshqa yopilgan zayavkalar yo'q.")
            await callback.answer()
            return
        lines = []
        for idx, order in enumerate(orders, start=offset + 1):
            lines.append(
                "\n".join(
                    [
                        f"{idx}. {format_order_message(order, include_id=False, include_address=False)}"
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
                            text="Yana 10 ta", callback_data=f"orders:closed:{offset + limit}"
                        )
                    ]
                ]
            )
        await callback.message.answer(message_text, reply_markup=keyboard)
        await callback.answer()

    @dp.message(F.text == "Mahsulot qo'shish")
    async def add_product_start(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await message.answer("Mahsulot nomini kiriting.")
        await state.set_state(AddProductStates.name)

    @dp.callback_query(F.data == "add_product")
    async def add_product_inline(callback: types.CallbackQuery, state: FSMContext) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await callback.message.answer("Mahsulot nomini kiriting.")
        await state.set_state(AddProductStates.name)
        await callback.answer()

    @dp.message(AddProductStates.name)
    async def add_product_name(message: types.Message, state: FSMContext) -> None:
        await state.update_data(name=message.text)
        await message.answer("Narxini kiriting (1 kg uchun).")
        await state.set_state(AddProductStates.price)

    @dp.message(AddProductStates.price)
    async def add_product_price(message: types.Message, state: FSMContext) -> None:
        price = parse_price(message.text)
        if price is None:
            await message.answer("Narxni to'g'ri kiriting (masalan: 12000).")
            return
        await state.update_data(price=price)
        await message.answer("Tavsifini kiriting.")
        await state.set_state(AddProductStates.description)

    @dp.message(AddProductStates.description)
    async def add_product_description(message: types.Message, state: FSMContext) -> None:
        await state.update_data(description=message.text)
        await message.answer(
            "Agar rasm bo'lsa yuboring (1 dona). Tugatish uchun 'Tugatish' tugmasini bosing.",
            reply_markup=add_product_photos_keyboard(),
        )
        await state.set_state(AddProductStates.photos)

    @dp.message(AddProductStates.photos)
    async def add_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if message.text and message.text.lower() == "mahsulotlar":
            await state.clear()
            await show_products(message)
            return
        if message.text and message.text.lower() == "tugatish":
            product_id = db.add_product(data["name"], data["price"], data["description"])
            if photos:
                db.set_product_photos(product_id, photos[:1])
            await message.answer("Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "Iltimos, rasm yuboring yoki 'Tugatish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        product_id = db.add_product(data["name"], data["price"], data["description"])
        db.set_product_photos(product_id, photos[:1])
        await message.answer("Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
        await state.clear()

    @dp.message(F.text == "Mahsulotni tahrirlash")
    async def edit_product_list(message: types.Message) -> None:
        if not is_admin(message.from_user.id):
            return
        products = db.list_products()
        if not products:
            await message.answer("Mahsulotlar mavjud emas.")
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
        await callback.message.answer("Nimani tahrirlaysiz?", reply_markup=edit_fields_keyboard())
        await state.set_state(EditProductStates.field)
        await callback.answer()

    @dp.callback_query(EditProductStates.field, F.data.startswith("field:"))
    async def edit_product_field(callback: types.CallbackQuery, state: FSMContext) -> None:
        field = callback.data.split(":", 1)[1]
        await state.update_data(field=field)
        if field == "photos":
            await callback.message.answer(
                "Yangi rasmni yuboring (1 dona). Tugatish: 'Tugatish' tugmasi.",
                reply_markup=add_product_photos_keyboard(),
            )
            await state.set_state(EditProductStates.photos)
        else:
            await callback.message.answer("Yangi qiymatni kiriting.")
            await state.set_state(EditProductStates.value)
        await callback.answer()

    @dp.message(EditProductStates.value)
    async def edit_product_value(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        product_id = data["product_id"]
        field = data["field"]
        if field == "name":
            db.update_product_name(product_id, message.text)
        elif field == "price":
            price = parse_price(message.text)
            if price is None:
                await message.answer("Narxni to'g'ri kiriting.")
                return
            db.update_product_price(product_id, price)
        elif field == "description":
            db.update_product_description(product_id, message.text)
        await message.answer("Mahsulot yangilandi.", reply_markup=user_keyboard(True))
        await state.clear()

    @dp.message(EditProductStates.photos)
    async def edit_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if message.text and message.text.lower() == "mahsulotlar":
            db.set_product_photos(data["product_id"], [])
            await state.clear()
            await show_products(message)
            return
        if message.text and message.text.lower() == "tugatish":
            if photos:
                db.set_product_photos(data["product_id"], photos[:1])
            else:
                db.set_product_photos(data["product_id"], [])
            await message.answer("Rasmlar yangilandi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer(
                "Iltimos, rasm yuboring yoki 'Tugatish' tugmasini bosing.",
                reply_markup=add_product_photos_keyboard(),
            )
            return
        photos = [message.photo[-1].file_id]
        await state.update_data(photos=photos[:1])
        db.set_product_photos(data["product_id"], photos[:1])
        await message.answer("Rasmlar yangilandi.", reply_markup=user_keyboard(True))
        await state.clear()

    @dp.message(F.text == "Рассылка")
    async def broadcast_start(message: types.Message, state: FSMContext) -> None:
        if not is_admin(message.from_user.id):
            return
        await message.answer("Рассылка uchun matn, foto yoki video yuboring.")
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
        await message.answer("Рассылkani tasdiqlaysizmi? (Ha/Yo'q)")
        await state.set_state(BroadcastStates.confirm)

    @dp.message(BroadcastStates.confirm)
    async def broadcast_confirm(message: types.Message, state: FSMContext) -> None:
        text = message.text.lower() if message.text else ""
        if text not in {"ha", "yo'q", "yoq", "нет", "да"}:
            await message.answer("Iltimos, Ha yoki Yo'q deb javob bering.")
            return
        if text in {"yo'q", "yoq", "нет"}:
            await message.answer("Рассылка bekor qilindi.")
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
            f"Рассылка yakunlandi. Muvaffaqiyatli: {success}, Xatolar: {failed}."
        )
        await state.clear()

    @dp.message()
    async def fallback(message: types.Message) -> None:
        if not await ensure_user_registered(message):
            return
        await message.answer("Iltimos, menyudan tanlang.")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())