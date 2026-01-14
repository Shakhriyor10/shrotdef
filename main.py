import asyncio
import logging
import os
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

ADMIN_LIST = {574853103, 506687945, 960217500, 688971244}

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
        rows.append([KeyboardButton(text="Statistika"), KeyboardButton(text="Рассылка")])
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


async def send_product(chat_id: int, product, bot: Bot, admin: bool) -> None:
    photos = db.get_product_photos(product["id"])
    caption = (
        f"Mahsulot: {product['name']}\n"
        f"Narxi (1 kg): {product['price_per_kg']}\n"
        f"Tavsif: {product['description'] or 'Kiritilmagan'}"
    )
    if photos:
        builder = MediaGroupBuilder()
        for index, file_id in enumerate(photos[:3]):
            builder.add_photo(media=file_id, caption=caption if index == 0 else None)
        await bot.send_media_group(chat_id=chat_id, media=builder.build())
        await bot.send_message(
            chat_id=chat_id,
            text="Buyurtma berish uchun pastdagi tugmani bosing.",
            reply_markup=product_inline_keyboard(product["id"], admin),
        )
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
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is required")

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
        db.add_order(user["id"], data["product_id"], data["quantity"], message.text)
        await message.answer("Arizangiz qabul qilindi!", reply_markup=user_keyboard(is_admin(message.from_user.id)))
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
            "Agar rasm bo'lsa yuboring (1-3 dona). Tugatish uchun 'Tugatish' deb yozing."
        )
        await state.set_state(AddProductStates.photos)

    @dp.message(AddProductStates.photos)
    async def add_product_photos(message: types.Message, state: FSMContext) -> None:
        data = await state.get_data()
        photos = data.get("photos", [])
        if message.text and message.text.lower() == "tugatish":
            product_id = db.add_product(data["name"], data["price"], data["description"])
            if photos:
                db.set_product_photos(product_id, photos[:3])
            await message.answer("Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer("Iltimos, rasm yuboring yoki 'Tugatish' deb yozing.")
            return
        photos.append(message.photo[-1].file_id)
        await state.update_data(photos=photos[:3])
        if len(photos) >= 3:
            product_id = db.add_product(data["name"], data["price"], data["description"])
            db.set_product_photos(product_id, photos[:3])
            await message.answer("Mahsulot qo'shildi.", reply_markup=user_keyboard(True))
            await state.clear()
        else:
            await message.answer("Yana rasm yuboring yoki 'Tugatish' deb yozing.")

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
            await callback.message.answer("Yangi rasmlarni yuboring (1-3 dona). Tugatish: 'Tugatish'.")
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
        if message.text and message.text.lower() == "tugatish":
            if photos:
                db.set_product_photos(data["product_id"], photos[:3])
            await message.answer("Rasmlar yangilandi.", reply_markup=user_keyboard(True))
            await state.clear()
            return
        if not message.photo:
            await message.answer("Iltimos, rasm yuboring yoki 'Tugatish' deb yozing.")
            return
        photos.append(message.photo[-1].file_id)
        await state.update_data(photos=photos[:3])
        if len(photos) >= 3:
            db.set_product_photos(data["product_id"], photos[:3])
            await message.answer("Rasmlar yangilandi.", reply_markup=user_keyboard(True))
            await state.clear()
        else:
            await message.answer("Yana rasm yuboring yoki 'Tugatish' deb yozing.")

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
