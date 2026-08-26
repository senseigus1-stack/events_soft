"""Telegram channel for Ваёбыж and the Кытчи recommendation engine."""

import asyncio
from html import escape

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select

from .config import get_settings
from .database import SessionLocal, create_schema
from .models import City, User
from .service import ensure_user, recommendations, record_interaction


dp = Dispatcher()


def city_keyboard() -> InlineKeyboardMarkup:
    with SessionLocal() as session:
        cities = list(session.scalars(select(City).where(City.active.is_(True)).order_by(City.name)))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=city.name, callback_data=f"city:{city.slug}")]
            for city in cities[:12]
        ]
    )


def event_keyboard(event_id: int, event_url: str) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(text="♡ Сохранить", callback_data=f"save:{event_id}"),
        InlineKeyboardButton(text="Дальше →", callback_data=f"next:{event_id}"),
    ]]
    if event_url:
        rows.append([InlineKeyboardButton(text="Открыть событие", url=event_url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_recommendation(message: Message, user_id: str) -> None:
    with SessionLocal() as session:
        user = ensure_user(session, user_id)
        session.commit()
        if not user.city_slug:
            await message.answer("Сначала выберите город:", reply_markup=city_keyboard())
            return
        ranked = recommendations(session, user_id, city=user.city_slug, limit=1)
    if not ranked:
        await message.answer("На ближайшие даты пока ничего не нашлось. Попробуйте позже.")
        return
    event, _, reasons = ranked[0]
    reason = f"\n\n✨ <i>{escape(reasons[0])}</i>" if reasons else ""
    price = "Бесплатно" if event.is_free else event.price_text or "Цена на сайте"
    text = (
        f"<b>{escape(event.title)}</b>\n"
        f"{event.starts_at:%d.%m · %H:%M}\n"
        f"{escape(event.venue_name or event.address)}\n"
        f"{escape(price)}{reason}"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=event_keyboard(event.id, event.event_url))


@dp.message(Command("start"))
async def start(message: Message) -> None:
    if not message.from_user:
        return
    with SessionLocal() as session:
        user = ensure_user(session, str(message.from_user.id))
        user.display_name = message.from_user.full_name
        session.commit()
    await message.answer(
        "Привет! Это Ваёбыж, а подборку делает Кытчи. Выберите город — затем пришлю первый вариант.",
        reply_markup=city_keyboard(),
    )


@dp.message(Command("city"))
async def choose_city(message: Message) -> None:
    await message.answer("Выберите город:", reply_markup=city_keyboard())


@dp.message(Command("recommend"))
async def recommend_command(message: Message) -> None:
    if message.from_user:
        await send_recommendation(message, str(message.from_user.id))


@dp.callback_query(F.data.startswith("city:"))
async def set_city(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.data or not callback.message:
        return
    slug = callback.data.split(":", 1)[1]
    with SessionLocal() as session:
        if session.get(City, slug) is None:
            await callback.answer("Город пока недоступен", show_alert=True)
            return
        user = ensure_user(session, str(callback.from_user.id))
        user.city_slug = slug
        session.commit()
    await callback.answer("Город сохранён")
    await send_recommendation(callback.message, str(callback.from_user.id))


@dp.callback_query(F.data.startswith("save:"))
async def save_event(callback: CallbackQuery) -> None:
    if not callback.data:
        return
    event_id = int(callback.data.split(":", 1)[1])
    with SessionLocal() as session:
        record_interaction(session, str(callback.from_user.id), event_id, "save")
    await callback.answer("Сохранено — это повлияет на следующие рекомендации")


@dp.callback_query(F.data.startswith("next:"))
async def next_event(callback: CallbackQuery) -> None:
    if callback.data:
        event_id = int(callback.data.split(":", 1)[1])
        with SessionLocal() as session:
            record_interaction(session, str(callback.from_user.id), event_id, "dismiss")
    if callback.message:
        await send_recommendation(callback.message, str(callback.from_user.id))
    await callback.answer()


async def run() -> None:
    settings = get_settings()
    if not settings.telegram_token:
        raise RuntimeError("TELEGRAM_TOKEN is required to run the bot")
    create_schema()
    bot = Bot(settings.telegram_token)
    await dp.start_polling(bot)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
