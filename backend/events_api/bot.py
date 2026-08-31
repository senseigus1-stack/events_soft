"""Production Telegram interface for Vayobyzh and the Kytchi AI feed."""

import asyncio
from datetime import datetime, timezone
from html import escape

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from sqlalchemy import select

from .community import set_attendance
from .config import get_settings
from .database import SessionLocal, create_schema
from .models import City, Event, Interaction, UserInterest
from .service import ensure_user, recommendations, record_interaction, replace_interests


dp = Dispatcher()

INTEREST_OPTIONS = {
    "music": "Музыка",
    "art": "Выставки",
    "theatre": "Театр",
    "cinema": "Кино",
    "talks": "Лекции",
    "festivals": "Фестивали",
    "kids": "С детьми",
    "sport": "Спорт",
}

STAGE_LABELS = {
    "exploring": "изучаю ваш вкус",
    "learning": "уже вижу закономерности",
    "personalized": "персональный режим активен",
}


def city_keyboard() -> InlineKeyboardMarkup:
    with SessionLocal() as session:
        cities = list(
            session.scalars(
                select(City).where(City.active.is_(True)).order_by(City.name)
            )
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=city.name, callback_data=f"city:{city.slug}")]
            for city in cities[:12]
        ]
    )


def interests_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(
            text=f"{'✓ ' if label in selected else ''}{label}",
            callback_data=f"interest:{key}",
        )
        for key, label in INTEREST_OPTIONS.items()
    ]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            buttons[index:index + 2] for index in range(0, len(buttons), 2)
        ] + [[InlineKeyboardButton(text="Готово →", callback_data="interest_done")]]
    )


def event_keyboard(event_id: int, event_url: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="♡ Сохранить", callback_data=f"save:{event_id}"),
            InlineKeyboardButton(text="● Пойду", callback_data=f"attend:{event_id}"),
        ],
        [InlineKeyboardButton(text="Не моё · дальше", callback_data=f"next:{event_id}")],
    ]
    if event_url:
        rows.append([InlineKeyboardButton(text="Открыть страницу события ↗", url=event_url)])
    settings = get_settings()
    if settings.public_site_url:
        rows.append([InlineKeyboardButton(text="Открыть всю ленту", url=settings.public_site_url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _selected_interests(user_id: str) -> set[str]:
    with SessionLocal() as session:
        return set(
            session.scalars(
                select(UserInterest.tag).where(UserInterest.user_id == user_id)
            )
        )


async def send_recommendation(message: Message, user_id: str) -> None:
    with SessionLocal() as session:
        user = ensure_user(session, user_id)
        session.commit()
        if not user.city_slug:
            await message.answer("Сначала выберите город:", reply_markup=city_keyboard())
            return
        batch = recommendations(session, user_id, city=user.city_slug, limit=1)
    if not batch.items:
        await message.answer(
            "На ближайшие даты подборка закончилась. Я уже учёл ваши ответы — загляните чуть позже."
        )
        return

    event, ranked = batch.items[0]
    reasons = "\n".join(f"· {escape(reason)}" for reason in ranked.reasons)
    price = "Бесплатно" if event.is_free else event.price_text or "Цена на сайте"
    venue = event.venue_name or event.address or "Место уточняется"
    text = (
        f"<b>{ranked.match_percent}% · {escape(event.title)}</b>\n"
        f"{event.starts_at:%d.%m · %H:%M}\n"
        f"{escape(venue)}\n"
        f"{escape(price)}\n\n"
        f"<b>Почему Кытчи выбрал это:</b>\n{reasons}\n\n"
        f"<i>{STAGE_LABELS[batch.stage]} · сигналов: {batch.signal_count}</i>"
    )
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=event_keyboard(event.id, event.event_url),
    )


@dp.message(Command("start"))
async def start(message: Message) -> None:
    if not message.from_user:
        return
    user_id = str(message.from_user.id)
    with SessionLocal() as session:
        user = ensure_user(session, user_id)
        user.display_name = message.from_user.full_name
        user.telegram_chat_id = str(message.chat.id)
        has_city = bool(user.city_slug)
        session.commit()
    await message.answer(
        "<b>Кытчи — ваш AI-навигатор по событиям.</b>\n\n"
        "Я учусь только на выбранных интересах и ваших действиях: что открыли, "
        "сохранили, пропустили или куда решили пойти. Каждый выбор объясняю.",
        parse_mode="HTML",
    )
    if not has_city:
        await message.answer("Где ищем события?", reply_markup=city_keyboard())
    else:
        await message.answer(
            "Проверим стартовые интересы. Отметьте всё, что откликается:",
            reply_markup=interests_keyboard(_selected_interests(user_id)),
        )


@dp.message(Command("city"))
async def choose_city(message: Message) -> None:
    await message.answer("Выберите город:", reply_markup=city_keyboard())


@dp.message(Command("interests"))
async def choose_interests(message: Message) -> None:
    if not message.from_user:
        return
    user_id = str(message.from_user.id)
    await message.answer(
        "Настройте интересы — выбор можно менять в любой момент:",
        reply_markup=interests_keyboard(_selected_interests(user_id)),
    )


@dp.message(Command("recommend"))
async def recommend_command(message: Message) -> None:
    if message.from_user:
        await send_recommendation(message, str(message.from_user.id))


@dp.message(Command("profile"))
async def profile_command(message: Message) -> None:
    if not message.from_user:
        return
    user_id = str(message.from_user.id)
    with SessionLocal() as session:
        user = ensure_user(session, user_id)
        batch = recommendations(session, user_id, city=user.city_slug, limit=1)
        city = session.get(City, user.city_slug) if user.city_slug else None
    summary = ", ".join(batch.profile_summary) or "пока собираю первые сигналы"
    await message.answer(
        f"<b>Профиль Кытчи</b>\n"
        f"Город: {escape(city.name if city else 'не выбран')}\n"
        f"Режим: {STAGE_LABELS[batch.stage]}\n"
        f"Уверенность: {round(batch.confidence * 100)}%\n"
        f"Сигналов: {batch.signal_count}\n"
        f"Сейчас замечаю: {escape(summary)}",
        parse_mode="HTML",
        reply_markup=interests_keyboard(_selected_interests(user_id)),
    )


@dp.message(Command("saved"))
async def saved_command(message: Message) -> None:
    if not message.from_user:
        return
    with SessionLocal() as session:
        events = list(
            session.scalars(
                select(Event)
                .join(Interaction, Interaction.event_id == Event.id)
                .where(
                    Interaction.user_id == str(message.from_user.id),
                    Interaction.action == "save",
                    Event.status == "published",
                    Event.starts_at >= datetime.now(timezone.utc),
                )
                .order_by(Event.starts_at)
                .limit(10)
            ).unique()
        )
    if not events:
        await message.answer("Сохранённых событий пока нет. Нажмите «Сохранить» под рекомендацией.")
        return
    lines = ["<b>Сохранённые события</b>"]
    for event in events:
        title = escape(event.title)
        if event.event_url:
            title = f'<a href="{escape(event.event_url, quote=True)}">{title}</a>'
        lines.append(f"\n{event.starts_at:%d.%m · %H:%M} — {title}")
    await message.answer("".join(lines), parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("help"))
async def help_command(message: Message) -> None:
    await message.answer(
        "/recommend — следующий персональный выбор\n"
        "/interests — настроить темы\n"
        "/profile — посмотреть, чему научился Кытчи\n"
        "/saved — сохранённые события\n"
        "/city — сменить город"
    )


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
        user.telegram_chat_id = str(callback.message.chat.id)
        session.commit()
    await callback.answer("Город сохранён")
    await callback.message.answer(
        "Теперь выберите несколько стартовых интересов:",
        reply_markup=interests_keyboard(_selected_interests(str(callback.from_user.id))),
    )


@dp.callback_query(F.data.startswith("interest:"))
async def toggle_interest(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.data or not callback.message:
        return
    key = callback.data.split(":", 1)[1]
    label = INTEREST_OPTIONS.get(key)
    if not label:
        await callback.answer("Неизвестный интерес")
        return
    user_id = str(callback.from_user.id)
    selected = _selected_interests(user_id)
    if label in selected:
        selected.remove(label)
    else:
        selected.add(label)
    with SessionLocal() as session:
        replace_interests(session, user_id, sorted(selected))
    await callback.message.edit_reply_markup(reply_markup=interests_keyboard(selected))
    await callback.answer("Настройки обновлены")


@dp.callback_query(F.data == "interest_done")
async def finish_interests(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    await callback.answer("Кытчи настроен")
    await send_recommendation(callback.message, str(callback.from_user.id))


@dp.callback_query(F.data.startswith("save:"))
async def save_event(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.data:
        return
    event_id = int(callback.data.split(":", 1)[1])
    with SessionLocal() as session:
        record_interaction(session, str(callback.from_user.id), event_id, "save")
    await callback.answer("Сохранено — Кытчи запомнил выбор")
    if callback.message:
        await send_recommendation(callback.message, str(callback.from_user.id))


@dp.callback_query(F.data.startswith("attend:"))
async def attend_event(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.data:
        return
    event_id = int(callback.data.split(":", 1)[1])
    with SessionLocal() as session:
        set_attendance(
            session,
            str(callback.from_user.id),
            event_id,
            attendance_status="going",
            reminder_minutes_before=1440,
        )
    await callback.answer("Добавлено в планы · напомню за день")
    if callback.message:
        await send_recommendation(callback.message, str(callback.from_user.id))


@dp.callback_query(F.data.startswith("next:"))
async def next_event(callback: CallbackQuery) -> None:
    if not callback.from_user:
        return
    if callback.data:
        event_id = int(callback.data.split(":", 1)[1])
        with SessionLocal() as session:
            record_interaction(
                session, str(callback.from_user.id), event_id, "dismiss"
            )
    await callback.answer("Учёл")
    if callback.message:
        await send_recommendation(callback.message, str(callback.from_user.id))


async def run() -> None:
    settings = get_settings()
    if not settings.telegram_token:
        raise RuntimeError("TELEGRAM_TOKEN is required to run the bot")
    create_schema()
    bot = Bot(settings.telegram_token)
    await bot.set_my_commands(
        [
            BotCommand(command="recommend", description="Получить рекомендацию"),
            BotCommand(command="interests", description="Настроить интересы"),
            BotCommand(command="profile", description="Профиль Кытчи"),
            BotCommand(command="saved", description="Сохранённые события"),
            BotCommand(command="city", description="Сменить город"),
            BotCommand(command="help", description="Все команды"),
        ]
    )
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
