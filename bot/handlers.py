from aiogram import F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, KeyboardButton, ReplyKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
import html
import re
from typing import Dict, Any
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import json


import logging
from datetime import datetime

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

import pytz

def format_moscow_time(unix_timestamp: int) -> str:
    """
    Переводит UNIX-timestamp в московское время и возвращает строку в красивом формате.
    
    Пример: "15 июня 2025, 14:30 (МСК)"
    """
    # 1. Создаём timezone-aware объект для Москвы
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    # 2. Преобразуем timestamp в datetime и привязываем часовой пояс
    dt = datetime.fromtimestamp(unix_timestamp, tz=moscow_tz)
    
    # 3. Форматируем в читаемый вид
    formatted = dt.strftime("%d %B %Y, %H:%M (МСК)")
    return formatted

def ensure_list_of_dicts(value, default=None):
    """
    Преобразует строку JSON или список в список словарей.
    Если значение пустое или невалидное — возвращает default.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            else:
                print(f"[WARNING] JSON не является списком: {parsed}")
                return default or []
        except json.JSONDecodeError:
            print(f"[ERROR] Не удалось декодировать JSON: {value}")
            return default or []
    return default or []

def serialize_for_db(value):
    """Сериализует список в JSON‑строку для сохранения в БД."""
    return json.dumps(value, ensure_ascii=False)

def clean_html(text: str) -> str:
    """Очищает HTML от неподдерживаемых Telegram тегов."""
    #Логика очистки написана здесь с помощью LLM модели 
    if not text:
        return ""
    
    # Заменяем <br> и аналоги на перенос строки
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    # Удаляем все остальные теги
    text = re.sub(r'<[^>]+>', '', text)
    # Экранируем спецсимволы
    return html.escape(text)

async def start(message: Message):
    db = message.bot.db
    user_id = message.from_user.id

    if db.get_user(user_id):
        await message.answer(
            "Привет! Я бот для рекомендаций мероприятий.\n"
            "Команды:\n"
            "/recommend — подборка событий\n"
            "/help — справка",
            reply_markup=ReplyKeyboardMarkup(keyboard=[])
        )
        return

    # Показываем клавиатуру с городам
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

    # Если пользователь уже зарегистрирован
    if db.get_user(user_id):
        return

    user_message = message.text.strip()
    user = message.from_user

    # # Имя (всегда есть)
    first_name = user.first_name

    # # Фамилия (может быть None)
    last_name = user.last_name

    # # Полное имя (объединяем, если фамилия есть)
    if last_name:
        full_name = f"{first_name} {last_name}"
    else:
        full_name = first_name

    if user_message in ["МСК", "СПБ", "МСК и СПБ"]:
        city_mapping = {"МСК": 1, "СПБ": 2, "МСК и СПБ": 3}
        selected_city = city_mapping[user_message]

        try:
            with db.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, name, city) VALUES (%s, %s, %s )",
                    (user_id, full_name, selected_city)
                )
            db.conn.commit()
            print(f"Пользователь {user_id}, {full_name} добавлен в БД с городом {selected_city}")

            await message.answer(
                f"Город «{user_message}» выбран! Теперь вы будете получать события для этого региона.\n\n"
                "Привет! Я бот для рекомендаций мероприятий.\n"
                "Команды:\n"
                "/recommend — подборка событий\n"
                "/help — справка",
                reply_markup=ReplyKeyboardMarkup(keyboard=[])
            )
        except Exception as e:
            print(f"Ошибка при добавлении пользователя {user_id}: {e}")
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

        # Определяем таблицы для поиска
        city = user.get("city")
        tables_to_search = ["msk"] if city == 1 else ["spb"] if city == 2 else ["msk", "spb"]

        all_candidates = []
        for table_name in tables_to_search:
            candidates = db.get_recommended_events(table_name=table_name, limit=50)
            all_candidates.extend(candidates)

        recommended = ml.recommend(user["event_history"], all_candidates)

        if not recommended:
            await message.answer("Пока нет рекомендаций. Оцените несколько событий!")
            return

        # Сохраняем список рекомендаций и индекс текущего события в состояние
        await state.update_data(
            recommended_events=recommended,
            current_index=0
        )

        # Показываем первое событие
        await show_event(message, bot, state)

    except Exception as e:
        print(f"[ERROR] В recommend для user_id={user_id}: {e}")
        await message.answer("Произошла ошибка при получении рекомендаций. Попробуйте ещё раз.")

async def show_event(message_or_callback, bot, state: FSMContext):
    data = await state.get_data()
    recommended = data["recommended_events"]
    current_index = data["current_index"]

    if current_index >= len(recommended):
        # Все события показаны
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text("Больше нет рекомендаций.")
        else:
            await message_or_callback.answer("Больше нет рекомендаций.")
        return

    event = recommended[current_index]

    # Проверяем обязательные поля
    if not all(key in event for key in ["id", "title", "event_url"]):
        await skip_invalid_event(message_or_callback, state, current_index)
        return

    # Формируем текст
    title = html.escape(event["title"])
    description = clean_html(event.get("description", ""))[:500]
    start_datetime = event.get("start_datetime", "")
    event_url = event["event_url"]
    start_datetime = format_moscow_time(start_datetime)
    
    text = (
        f"<b>{title}</b>\n"
        f"\n"
        f"{description}...\n"
        f"\n"
        f"📅 {start_datetime}\n"
        f"\n"
        f"<a href='{event_url}'>Подробнее</a>"
    )

    # Создаём клавиатуру
    keyboard = InlineKeyboardBuilder()
    keyboard.add(
        InlineKeyboardButton(text="👍", callback_data=f"like_{event['id']}"),
        InlineKeyboardButton(text="👎", callback_data=f"dislike_{event['id']}")
    )
    if current_index < len(recommended) - 1:
        keyboard.add(InlineKeyboardButton(
            text="Следующее",
            callback_data=f"next_{current_index + 1}"
        ))

    # Отправляем или редактируем сообщение
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
            event_id_str = data.split("_")[1]
            event_id = int(event_id_str)

            db.add_event_to_history(user_id, event_id, "like")

            data_state = await state.get_data()
            recommended = data_state.get("recommended_events", [])
            event = next(
                (e for e in recommended if str(e["id"]) == str(event_id)),
                None
            )

            if event:
                user_status = ensure_list_of_dicts(user["status_ml"], default=[])
                event_status = ensure_list_of_dicts(event["status_ml"], default=[])

                try:
                    new_status_ml = ml.update_user_status_ml(
                        user_status, event_status, weight=0.3
                    )
                    
                    # Логируем факт сохранения
                    logger.info(
                        f"Пользователь {user_id} получил новый статус после лайка события {event_id}. "
                        f"Обновлённые категории: {[c['category'] for c in new_status_ml]}"
                    )
                    
                    db.update_user_status_ml(user_id, serialize_for_db(new_status_ml))
                except Exception as e:
                    logger.error(f"Ошибка при обновлении статуса для {user_id}: {e}")

            await callback.answer("Спасибо за оценку! 😊")
            await next_event(callback, bot, state)

        elif data.startswith("dislike_"):
            # Аналогично для dislike (можно добавить логику понижения весов)
            event_id = int(data.split("_")[1])
            db.add_event_to_history(user_id, event_id, "dislike")
            await callback.answer("Не будем показывать такое. 😐")
            await next_event(callback, bot, state)

        elif data.startswith("next_"):
            new_index = int(data.split("_")[1])
            await state.update_data(current_index=new_index)
            await show_event(callback, bot, state)

    except Exception as e:
        print(f"[ERROR] В button_handler: {e}")
        await callback.answer("Произошла ошибка. Попробуйте ещё раз.")

async def next_event(callback: CallbackQuery, bot, state: FSMContext):
    data = await state.get_data()
    current_index = data["current_index"]
    new_index = current_index + 1

    await state.update_data(current_index=new_index)
    await show_event(callback, bot, state)



async def skip_invalid_event(message_or_callback, state: FSMContext, current_index):
    await state.update_data(current_index=current_index + 1)
    await show_event(message_or_callback, state)