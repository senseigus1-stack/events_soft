
from aiogram import F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, KeyboardButton, ReplyKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
import html
import re
from typing import Dict, Any, List
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import json
import hashlib
import logging
from datetime import datetime
import pytz

# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("status_updates.log", encoding="utf-8"),
        logging.StreamHandler()  # вывод в консоль
    ]
)
logger = logging.getLogger(__name__)

# Определяем состояние
class RecommendationState(StatesGroup):
    showing = State()

def format_moscow_time(unix_timestamp: int) -> str:
    """Переводит UNIX-timestamp в московское время."""
    moscow_tz = pytz.timezone('Europe/Moscow')
    dt = datetime.fromtimestamp(unix_timestamp, tz=moscow_tz)
    return dt.strftime("%d %B %Y, %H:%M (МСК)")

def ensure_list_of_dicts(value, default=None):
    """Преобразует JSON-строку или список в список словарей."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else (default or [])
        except json.JSONDecodeError:
            logger.error(f"Не удалось декодировать JSON: {value}")
            return default or []
    return default or []

def serialize_for_db(value):
    """Сериализует список в JSON‑строку для БД."""
    return json.dumps(value, ensure_ascii=False)

def clean_html(text: str) -> str:
    """Очищает HTML от неподдерживаемых Telegram тегов."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    return html.escape(text)

async def start(message: Message):
    db = message.bot.db
    user_id = message.from_user.id
    if db.get_user(user_id):
        await message.answer(
            "Привет! Я бот для рекомендаций мероприятий.\n"
            "Команды:\n"
            "   /recommend — подборка событий\n"
            "   /referral — добавить друга\n"
            "   /add — предложить свое мероприятие\n"
            "   /help — справка",
            reply_markup=ReplyKeyboardMarkup(keyboard=[])
        )
        return
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(KeyboardButton(text="МСК"), KeyboardButton(text="СПБ"), KeyboardButton(text="МСК и СПБ"))
    keyboard.adjust(1)
    await message.answer(
        "Выберите город, от которого будете получать события:",
        reply_markup=keyboard.as_markup(resize_keyboard=True)
    )

async def handle_city_selection(message: Message):
    db = message.bot.db
    user_id = message.from_user.id
    if db.get_user(user_id):
        return
    user_message = message.text.strip()
    user = message.from_user
    full_name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
    if user_message in ["МСК", "СПБ", "МСК и СПБ"]:
        city_mapping = {"МСК": 1, "СПБ": 2, "МСК и СПБ": 3}
        selected_city = city_mapping[user_message]
        try:
            with db.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, name, city) VALUES (%s, %s, %s)",
                    (user_id, full_name, selected_city)
                )
            db.conn.commit()
            logger.info(f"Пользователь {user_id} добавлен в БД с городом {selected_city}")
            await message.answer(
                f"Город «{user_message}» выбран! Теперь вы будете получать события для этого региона.\n\n"
                "Привет! Я бот для рекомендаций мероприятий.\n"
                "Команды:\n"
                "   /recommend — подборка событий\n"
                "   /referral — добавить друга\n"
                "   /add — предложить свое мероприятие\n"
                "   /help — справка",
                reply_markup=ReplyKeyboardMarkup(keyboard=[])
            )
        except Exception as e:
            logger.error(f"Ошибка при добавлении пользователя {user_id}: {e}")
            await message.answer("Произошла ошибка при регистрации. Попробуйте ещё раз.")

async def recommend(message: Message, bot, state: FSMContext):
    db = bot.db
    ml = bot.ml
    user_id = message.from_user.id

    try:
        user = db.get_user(user_id)
        if not user:
            await message.answer("Сначала напишите /start")
            return

        city = user.get("city")
        tables_to_search = ["msk"] if city == 1 else ["spb"] if city == 2 else ["msk", "spb"]

        all_candidates = []
        for table_name in tables_to_search:
            candidates = db.get_recommended_events(table_name=table_name, limit=50)
            logger.info(f"Найдено событий в {table_name}: {len(candidates)}")  # Отладка
            all_candidates.extend(candidates)

        logger.info(f"Всего кандидатов: {len(all_candidates)}")  # Отладка

        recommended = ml.recommend(user["event_history"], all_candidates)
        logger.info(f"Рекомендовано событий: {len(recommended)}")  # Отладка

        if not recommended:
            await message.answer(
                "Пока нет рекомендаций. Оцените несколько событий!\n"
                "Попробуйте позже или измените город в настройках."
            )
            return

        await state.update_data(recommended_events=recommended, current_index=0)
        await show_event(message, state)

    except Exception as e:
        logger.error(f"[ERROR] В recommend для user_id={user_id}: {e}")
        await message.answer("Произошла ошибка при получении рекомендаций. Попробуйте ещё раз.")



async def show_event(message_or_callback: Message | CallbackQuery, state: FSMContext, attempt=0):
    if attempt > 10:
        logger.error("[show_event] Превышено число попыток найти валидное событие")
        await message_or_callback.answer("Произошла ошибка при показе рекомендаций.")
        return

    data = await state.get_data()
    recommended: List[Dict] = data.get("recommended_events", [])
    current_index: int = data.get("current_index", 0)

    logger.info(f"[show_event] attempt={attempt}, current_index={current_index}, len={len(recommended)}")

    if current_index >= len(recommended):
        text = (
            "Больше нет рекомендаций.\n\n"
            "Попробуйте:\n"
            "- Оценить другие события (❤️/👎)\n"
            "- Изменить город в настройках"
        )
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    event = recommended[current_index]
    logger.info(f"[show_event] Событие №{current_index}: {event}")

    required_keys = ["id", "title", "event_url"]
    if not all(key in event for key in required_keys):
        logger.warning(f"[show_event] Пропускаем событие №{current_index} (нет полей: {required_keys})")
        await state.update_data(current_index=current_index + 1)
        await show_event(message_or_callback, state, attempt + 1)
        return
    
    title = html.escape(event["title"])
    description = clean_html(event.get("description", ""))[:500]
    start_datetime = format_moscow_time(event.get("start_datetime", 0))
    event_url = event["event_url"]
    event_id = event["id"]
    text = (
        f"<b>{title}</b>\n\n"
        f"{description}...\n\n"
        f"📅 {start_datetime}\n\n"
        f"<a href='{event_url}'>Подробнее</a>"
    )
    keyboard = InlineKeyboardBuilder()
    keyboard.add(
        InlineKeyboardButton(text="❤️", callback_data=f"like_{event_id}"),
        InlineKeyboardButton(text="👎", callback_data=f"dislike_{event_id}")
    )
    keyboard.add(
        InlineKeyboardButton(
            text="✅Пойду!",
            callback_data=f"confirm_go_{event_id}"
        )
    )
    if current_index < len(recommended) - 1:
        keyboard.add(
            InlineKeyboardButton(
                text="Следующее",
                callback_data=f"next_{current_index + 1}"
            )
        )
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            text=text,
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML"
        )
    else:
        await message_or_callback.answer(
            text=text,
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML"
        )

async def button_handler(callback: CallbackQuery, bot, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    db = bot.db
    ml = bot.ml

    user = db.get_user(user_id)
    if not user:
        await callback.answer("Ошибка: пользователь не найден.")
        await callback.message.edit_reply_markup(None)
        return

    try:
        if data.startswith("like_"):
            event_id = int(data.split("_")[1])
            db.add_event_to_history(user_id, event_id, "like")
            data_state = await state.get_data()
            recommended = data_state.get("recommended_events", [])
            event = next((e for e in recommended if str(e["id"]) == str(event_id)), None)
            if event:
                user_status = ensure_list_of_dicts(user["status_ml"], default=[])
                event_status = ensure_list_of_dicts(event["status_ml"], default=[])
                try:
                    new_status_ml = ml.update_user_status_ml(user_status, event_status, weight=0.3)
                    logger.info(
                        f"Пользователь {user_id} обновил статус после лайка события {event_id}. "
                        f"Категории: {[c['category'] for c in new_status_ml]}"
                    )
                    db.update_user_status_ml(user_id, serialize_for_db(new_status_ml))
                except Exception as e:
                    logger.error(f"Ошибка при обновлении статуса ML для {user_id}: {e}")
            await callback.answer("😊")
            await next_event(callback, state)


        elif data.startswith("dislike_"):
            event_id = int(data.split("_")[1])
            db.add_event_to_history(user_id, event_id, "dislike")
            await callback.answer("Продолжаем формировать рекомендации. 😐")
            await next_event(callback, state)


        elif data.startswith("next_"):
            new_index = int(data.split("_")[1])
            await state.update_data(current_index=new_index)
            await show_event(callback, state)


        elif data.startswith("confirm_go_"):
            parts = data.split("_")
            # Проверяем, что частей ровно 3: ['confirm', 'go', '222565']
            if len(parts) != 3:
                await callback.answer("Ошибка данных. Попробуйте ещё раз.")
                return

            try:
                event_id = int(parts[2])  # Было: parts[1], теперь parts[2]
            except ValueError:
                await callback.answer("Некорректный ID события.")
                return

            success = db.confirm_event(user_id, event_id)
            if success:
                await callback.answer("Вы подтвердили участие! 😊")
                await show_event(callback, state)
            else:
                await callback.answer("Не удалось подтвердить участие. Попробуйте позже.")



        else:
            logger.warning(f"Неизвестный callback_data от {user_id}: {data}")
            await callback.answer("Неизвестная команда. Попробуйте ещё раз.")


    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в числе для {user_id}, data={data}: {e}")
        await callback.answer("Ошибка обработки данных. Попробуйте снова.")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка в button_handler для {user_id}: {e}")
        await callback.answer("Произошла ошибка. Попробуйте ещё раз.")


async def next_event(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_index = data["current_index"]
    new_index = current_index + 1
    await state.update_data(current_index=new_index)
    await show_event(callback, state)

async def skip_invalid_event(message_or_callback, state: FSMContext, current_index):
    await state.update_data(current_index=current_index + 1)
    await show_event(message_or_callback, state)


async def show_referral(message: Message):
    db = message.bot.db
    user_id = message.from_user.id
    username = message.from_user.username or "user"
    referral_code = hashlib.md5(f"{user_id}{username}".encode()).hexdigest()[:10]
    success = db.save_referral_code(user_id, referral_code)
    if not success:
        await message.answer("У вас уже есть реферальная ссылка!")
        return
    bot_username = (await message.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{referral_code}"
    text = (
        "🔗 Ваша реферальная ссылка:\n\n"
        f"{referral_link}\n\n"
        "Пригласите друга и вы увидите мероприятия друг друга!"
    )
    await message.answer(text)