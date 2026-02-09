from aiogram.exceptions import TelegramBadRequest
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
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Состояния
class RecommendationState(StatesGroup):
    showing = State()

class EventStates(StatesGroup):
    waiting_for_event_data = State()
    confirming = State()

class InviteState(StatesGroup):
    selecting_event = State()      # Выбор мероприятия для приглашения
    selecting_friends = State()   # Выбор друзей для приглашения


def format_moscow_time(unix_timestamp: int) -> str:
    """Переводит UNIX-timestamp в московское время."""
    logger.debug(f"[format_moscow_time] Получен timestamp: {unix_timestamp}")
    moscow_tz = pytz.timezone('Europe/Moscow')
    dt = datetime.fromtimestamp(unix_timestamp, tz=moscow_tz)
    formatted = dt.strftime("%d %B %Y, %H:%M (МСК)")
    logger.info(f"[format_moscow_time] Преобразован в: {formatted}")
    return formatted

def ensure_list_of_dicts(value, default=None):
    """Преобразует JSON-строку или список в список словарей."""
    logger.debug(f"[ensure_list_of_dicts] Входное значение: {value}, default: {default}")
    if isinstance(value, list):
        logger.debug("[ensure_list_of_dicts] Значение уже список — возвращаем как есть.")
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            logger.debug(f"[ensure_list_of_dicts] JSON успешно декодирован: {parsed}")
            if isinstance(parsed, list):
                logger.info("[ensure_list_of_dicts] Декодированный JSON — список, возвращаем.")
                return parsed
            else:
                logger.warning("[ensure_list_of_dicts] JSON не является списком, используем default.")
                return default or []
        except json.JSONDecodeError as e:
            logger.error(f"[ensure_list_of_dicts] Ошибка декодирования JSON: {e}, value: {value}")
            return default or []
    logger.warning(f"[ensure_list_of_dicts] Тип значения не поддерживается: {type(value)}, возвращаем default.")
    return default or []

def serialize_for_db(value):
    """Сериализует список в JSON‑строку для БД."""
    logger.debug(f"[serialize_for_db] Сериализация значения: {value}")
    result = json.dumps(value, ensure_ascii=False)
    logger.info(f"[serialize_for_db] Результат: {result}")
    return result

def clean_html(text: str) -> str:
    """Очищает HTML от неподдерживаемых Telegram тегов."""
    logger.debug(f"[clean_html] Исходный текст: {text}")
    if not text:
        logger.info("[clean_html] Текст пуст, возвращаем пустую строку.")
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    cleaned = html.escape(text)
    logger.info(f"[clean_html] Очищенный текст: {cleaned}")
    return cleaned

# --- ОСНОВНЫЕ КОМАНДЫ ---


async def start(message: Message, state: FSMContext):
    logger.info(f"[start] Запуск команды /start для user_id={message.from_user.id}")
    db = message.bot.db
    user_id = message.from_user.id
    username = message.from_user.username or "user"

    await state.clear()  # Сброс состояния
    logger.debug(f"[start] Состояние FSM очищено для user_id={user_id}")


    # Обработка реферального кода
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        referral_code = args[1][4:]
        logger.info(f"[start] Обнаружен реферальный код: {referral_code}")
        try:
            referrer_id = db.get_user_by_referral_code(referral_code)
            if referrer_id and referrer_id != user_id:
                # Всегда пытаемся добавить дружбу
                try:
                    db.add_friend(user_id, referrer_id)
                    logger.info(f"[start] Друг добавлен: {user_id} ↔ {referrer_id}")
                except Exception as e:
                    logger.error(f"[start] Ошибка при добавлении в друзья: {e}")

                # Если пользователь ещё не в БД — добавляем
                if not db.get_user(user_id):
                    full_name = f"{message.from_user.first_name} {message.from_user.last_name}" \
                        if message.from_user.last_name else message.from_user.first_name
                    with db.conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO users (id, name) VALUES (%s, %s)",
                            (user_id, full_name)
                        )
                    db.conn.commit()
                    logger.info(f"[start] Пользователь {user_id} добавлен в БД с именем {full_name}")

                # Добавляем реферральную запись
                if not db.is_already_referred(user_id, referrer_id):
                    success = db.add_referral(user_id, referrer_id, referral_code)
                    if success:
                        await message.answer(
                            f"🎉 Вы, {full_name}, присоединились по реферальной ссылке от {referrer_id}!\n"
                            "Теперь вы дружите с ним."
                        )
                        logger.info(f"[start] Реферальная запись добавлена для {user_id} ← {referrer_id}")
                    else:
                        await message.answer("❌ Ошибка при обработке реферала.")
                        logger.error(f"[start] Не удалось добавить реферральную запись для {user_id}")
                else:
                    await message.answer("Вы уже были приглашены этим пользователем (дружба установлена).")
                    logger.info(f"[start] Реферальная связь уже существует: {user_id} ← {referrer_id}")
            else:
                await message.answer("Некорректный реферальный код.")
                logger.warning(f"[start] Реферальный код недействителен или совпадает с user_id: {referral_code}")
        except Exception as e:
            logger.error(f"[start] Ошибка обработки реферала: {e}")
            await message.answer("Произошла ошибка. Попробуйте позже.")


    # Проверка наличия пользователя в БД
    try:
        user = db.get_user(user_id)
        if user:
            await show_main_menu(message)
            logger.info(f"[start] Пользователь {user_id} найден в БД, показано главное меню.")
        else:
            await ask_city(message)
            logger.info(f"[start] Пользователь {user_id} не найден, запрошен выбор города.")
    except Exception as e:
        logger.error(f"[start] Ошибка запроса пользователя {user_id}: {e}")
        await message.answer("Произошла ошибка. Перезапустите бота (/start).")

async def ask_city(message: Message):
    logger.info(f"[ask_city] Запрос выбора города для user_id={message.from_user.id}")
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(
        KeyboardButton(text="Москва"),
        KeyboardButton(text="Санкт‑Петербург"),
        KeyboardButton(text="Оба города")
    )
    keyboard.adjust(1)
    await message.answer(
        "👋 Добро пожаловать! Выберите город для поиска мероприятий:\n\n"
        "<b>Варианты:</b>\n"
        "• Москва\n"
        "• Санкт‑Петербург\n"
        "• Оба города\n\n"
        "После выбора откроются все функции бота!",
        parse_mode="HTML",
        reply_markup=keyboard.as_markup(resize_keyboard=True, one_time_keyboard=True)
    )
    logger.debug(f"[ask_city] Клавиатура с городами отправлена пользователю {message.from_user.id}")

async def handle_city_selection(message: Message):
    logger.info(f"[handle_city_selection] Обработка выбора города от user_id={message.from_user.id}")
    db = message.bot.db
    user_id = message.from_user.id

    if db.get_user(user_id):  # Если пользователь уже есть — сразу меню
        await show_main_menu(message)
        logger.info(f"[handle_city_selection] Пользователь {user_id} уже в БД, показано главное меню.")
        return

    city_mapping = {"Москва": 1, "Санкт‑Петербург": 2, "Оба города": 3}
    selected = message.text.strip()

    if selected not in city_mapping:
        await message.answer("Выберите город из кнопок ниже.")
        logger.warning(f"[handle_city_selection] Некорректный выбор города: {selected} от user_id={user_id}")
        return

    city_id = city_mapping[selected]
    full_name = (
        f"{message.from_user.first_name} {message.from_user.last_name}"
        if message.from_user.last_name
        else message.from_user.first_name
    )

    try:
        with db.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, name, city) VALUES (%s, %s, %s)",
                (user_id, full_name, city_id)
            )
        db.conn.commit()
        logger.info(f"Пользователь {user_id} добавлен в БД с городом {city_id}, имя: {full_name}")
        await show_main_menu(message)
    except Exception as e:
        logger.error(f"Ошибка регистрации {user_id}: {e}")
        await message.answer("Ошибка при регистрации. Попробуйте ещё раз.")

async def show_main_menu(message: Message):
    """Показывает главное меню с кликабельными командами."""
    logger.info(f"[show_main_menu] Отображение главного меню для user_id={message.from_user.id}")
    await message.answer(
        "👋 Привет! Я бот для рекомендаций мероприятий.\n\n"
        "<b>Доступные команды:</b>\n\n"
        "🔸 /recommend — рекомендации событий\n"
        "🔸 /referral — реферальная ссылка\n"
        "🔸 /add — предложить мероприятие\n"
        "🔸 /help — справка\n\n"
        "   Скоро в функционале:\n\n"
        "       🔸/myfriends — список друзей\n"
        "       🔸/friendevents — мероприятия друга\n"
        "       🔸/invite — отправить приглашение друзьям на мероприятие\n\n"
        "Нажмите на команду, чтобы использовать её.",
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    logger.debug(f"[show_main_menu] Главное меню отправлено пользователю {message.from_user.id}")


# --- РЕКОМЕНДАЦИИ ---


async def recommend(message: Message, bot, state: FSMContext):
    logger.info(f"[recommend] Запуск рекомендации для user_id={message.from_user.id}")
    db = bot.db
    ml = bot.ml
    user_id = message.from_user.id

    try:
        user = db.get_user(user_id)
        if not user:
            await message.answer("Сначала напишите /start")
            logger.warning(f"[recommend] Пользователь {user_id} не найден в БД.")
            return

        # События, с которыми пользователь взаимодействовал
        interacted = {
            action["event_id"] for action in user.get("event_history", [])
        }
        logger.debug(f"[recommend] События, с которыми взаимодействовал пользователь: {interacted}")

        # Определяем таблицы для поиска
        city = user.get("city")
        tables = ["msk"] if city == 1 else ["spb"] if city == 2 else ["msk", "spb"]
        logger.info(f"[recommend] Таблицы для поиска: {tables}, город пользователя: {city}")


        # Собираем кандидатов из всех таблиц
        all_candidates = []
        for table in tables:
            candidates = db.get_recommended_events(
                table_name=table,
                limit=50,
                exclude_event_ids=interacted
            )
            all_candidates.extend(candidates)
            logger.debug(f"[recommend] Найдено {len(candidates)} кандидатов из таблицы {table}")


        # ML‑рекомендация
        recommended = ml.recommend(user.get("event_history", []), all_candidates)
        logger.info(f"[recommend] Рекомендовано: {len(recommended)} событий")

        if not recommended:
            await message.answer(
                "Пока нет рекомендаций. Оцените несколько событий!\n"
                "Попробуйте позже или смените город в настройках."
            )
            logger.info(f"[recommend] Нет рекомендаций для user_id={user_id}")
            return

        # === ПОЛУЧАЕМ ДАННЫЕ МЕСТ ДЛЯ ВСЕХ РЕКОМЕНДОВАННЫХ СОБЫТИЙ ===
        enhanced_recommended = []
        for event in recommended:
            event_id = event["id"]
            place_data = None

            # Определяем, из какой таблицы брать place_id (можно уточнить логику)
            # Здесь берём первую подходящую таблицу из tables
            for table in tables:
                try:
                    place_data = db.get_place_by_event_id(event_id, table)
                    if place_data:
                        break  # Нашли — выходим из цикла
                except Exception as e:
                    logger.warning(f"[recommend] Не удалось получить место для event_id={event_id} из таблицы {table}: {e}")

            # Добавляем place_data к событию
            enhanced_event = {**event, "place_data": place_data}
            enhanced_recommended.append(enhanced_event)

        # Сохраняем в state: уже с прикреплёнными данными места
        await state.update_data(
            recommended_events=enhanced_recommended,
            current_index=0
        )

        await show_event(message, state)
        logger.info(f"[recommend] Данные сохранены в FSM, запущено отображение событий для {user_id}")


    except Exception as e:
        logger.error(f"[recommend] Ошибка для {user_id}: {e}", exc_info=True)
        await message.answer("Ошибка получения рекомендаций. Повторите попытку.")


async def show_event(
    message_or_callback: Message | CallbackQuery,
    state: FSMContext,
    attempt: int = 0
):
    logger.debug(f"[show_event] Попытка отображения события, attempt={attempt}")
    if attempt > 10:
        logger.error("[show_event] Превышено число попыток отображения события.")
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.answer("Ошибка показа рекомендаций.")
        else:
            await message_or_callback.answer("Ошибка показа рекомендаций.")
        return

    data = await state.get_data()
    recommended: list[dict] = data.get("recommended_events", [])
    current_index: int = data.get("current_index", 0)

    if current_index >= len(recommended):
        msg = "Больше нет рекомендаций."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.answer(msg)
        else:
            await message_or_callback.answer(msg)
        logger.info("[show_event] Все рекомендации показаны.")
        return

    event = recommended[current_index]
    event_id = event["id"]

    # Берём place_data прямо из события (уже загружено в recommend)
    place_data = event.get("place_data")

    # Формируем текст сообщения
    title = event.get("title", "Без названия")
    desc = clean_html(event.get("description", ""))
    start_dt = format_moscow_time(event["start_datetime"])
    url = event.get("event_url", "#")

    text = f"<b>{title}</b>\n\n"
    if desc:
        text += f"{desc}\n\n"
    text += f"<i>Начало:</i> {start_dt}\n"
    text += f"<i>Ссылка:</i> <a href='{url}'>Перейти</a>\n"

    # Добавляем информацию о месте, если есть
    if place_data:
        place_title = place_data.get("title", "Не указано")
        place_address = place_data.get("address", "Адрес не указан")
        place_site = place_data.get("site_url", "")

        text += f"\n<b>Место:</b> {place_title}\n"
        text += f"<i>Адрес:</i> {place_address}\n"
        if place_site:
            text += f"<i>Сайт:</i> <a href='{place_site}'>Перейти</a>\n"
    else:
        text += "\n<i>Место не указано.</i>\n"

    text += "\nОцените событие:\n"

    # Клавиатура
    keyboard = InlineKeyboardBuilder()
    keyboard.add(
        InlineKeyboardButton(text="👍", callback_data=f"like_{event_id}"),
        InlineKeyboardButton(text="👎", callback_data=f"dislike_{event_id}"),
        InlineKeyboardButton(text="✅", callback_data=f"confirm_{event_id}")
    )
    keyboard.add(
        InlineKeyboardButton(
            text="➡️ Следующее",
            callback_data=f"next_{current_index + 1}"
        )
    )
    keyboard.adjust(3, 1)

    logger.debug(f"[show_event] Сформирована клавиатура для события {event_id}, индекс: {current_index}")

    try:
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard.as_markup(),
                disable_web_page_preview=False
            )
            logger.info(f"[show_event] Сообщение обновлено через CallbackQuery для event_id={event_id}")
        else:
            await message_or_callback.answer(
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard.as_markup(),
                disable_web_page_preview=False
            )
            logger.info(f"[show_event] Сообщение отправлено через Message для event_id={event_id}")
    except Exception as e:
        logger.error(f"[show_event] Ошибка отправки сообщения для event_id={event_id}: {e}")
        await show_event(message_or_callback, state, attempt + 1)


async def button_handler(callback: CallbackQuery, bot, state: FSMContext):
    logger.info(f"[button_handler] Обработка callback от user_id={callback.from_user.id}, data='{callback.data}'")
    user_id = callback.from_user.id
    data = callback.data
    db = bot.db
    ml = bot.ml

    user = db.get_user(user_id)
    if not user:
        await callback.answer("Ошибка: пользователь не найден.")
        await callback.message.edit_reply_markup(None)
        logger.warning(f"[button_handler] Пользователь {user_id} не найден в БД.")
        return

    try:
        if data.startswith("like_"):
            event_id = int(data.split("_")[1])
            logger.info(f"[button_handler] Пользователь {user_id} поставил лайк событию {event_id}")
            db.add_event_to_history(user_id, event_id, "like")
            db.increment_event_likes(event_id, 'spb')
            db.increment_event_likes(event_id, 'msk')

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
                    logger.error(f"Ошибка при обновлении статуса ML для {user_id}: {e}", exc_info=True)


            await callback.answer("Учтем в рекомендациях 😊")
            await next_event(callback, state)
            logger.info(f"[button_handler] Переход к следующему событию после лайка для {user_id}")


        elif data.startswith("dislike_"):
            event_id = int(data.split("_")[1])
            logger.info(f"[button_handler] Пользователь {user_id} поставил дизлайк событию {event_id}")
            db.add_event_to_history(user_id, event_id, "dislike")
            await callback.answer("Продолжаем формировать рекомендации. 😐")
            await next_event(callback, state)
            logger.info(f"[button_handler] Переход к следующему событию после дизлайка для {user_id}")


        elif data.startswith("next_invite_"):
            try:
                new_index_str = data.split("_")[2]  # "next_invite_5" → [2] = "5"
                if not new_index_str.isdigit():
                    raise ValueError(f"Некорректный индекс: {new_index_str}")
                new_index = int(new_index_str)

                await state.update_data(current_invite_index=new_index)
                await show_invite_event(callback, state)
                await callback.answer("Переходим к следующему мероприятию...")
                logger.info(f"[button_handler] Переход к приглашению следующего мероприятия (индекс {new_index})")
            except (ValueError, IndexError) as e:
                logger.error(f"[next_invite] Ошибка разбора: {e}")
                await callback.answer("Ошибка перехода к следующему мероприятию.")


        elif data.startswith("invite_to_event_"):
            try:
                event_id_str = data.split("_")[2]
                if not event_id_str.isdigit():
                    raise ValueError("Некорректный ID мероприятия")
                event_id = int(event_id_str)

                await state.update_data(pending_invite_event_id=event_id)
                await handle_select_event_for_invite(callback, bot, state)
                logger.info(f"[button_handler] Начато приглашение на событие {event_id} для {user_id}")
            except (ValueError, IndexError) as e:
                logger.error(f"[invite_to_event] Ошибка: {e}")
                await callback.answer("Не удалось начать приглашение. Попробуйте снова.")

        elif data.startswith("invite_event_"):
            await handle_invite_event(callback, bot, state)
            logger.info(f"[button_handler] Обработано приглашение на событие от {user_id}")

        elif data.startswith("accept_invite_"):
            await handle_accept_invite(callback, bot)
            logger.info(f"[button_handler] Принято приглашение от {callback.from_user.id}")


        elif data.startswith("decline_invite_"):
            await handle_decline_invite(callback, bot)
            logger.info(f"[button_handler] Отклонено приглашение от {callback.from_user.id}")


        elif data.startswith("confirm_"):  # Исправлено: было "confirm_go_"
            try:
                event_id_str = data.split("_")[1]  # Для "confirm_123" → "123"
                if not event_id_str.isdigit():
                    raise ValueError("Некорректный ID события")
                event_id = int(event_id_str)

                success = db.confirm_event(user_id, event_id)
                if success:
                    await callback.answer("Вы подтвердили участие! 😊")
                    # Переход к следующему событию
                    data_state = await state.get_data()
                    new_index = data_state.get("current_index", 0) + 1
                    await state.update_data(current_index=new_index)
                    recommended = data_state.get("recommended_events", [])
                    if new_index < len(recommended):
                        await show_event(callback, state)
                        logger.info(f"[button_handler] Переход к следующему рекомендованному событию (индекс {new_index})")
                    else:
                        await callback.message.edit_text("Больше нет рекомендаций.", reply_markup=None)
                        await callback.answer("Конец списка рекомендаций.")
                        logger.info("[button_handler] Рекомендаций больше нет, завершение показа.")
                else:
                    await callback.answer("Ошибка подтверждения. Попробуйте позже.")
                    logger.error(f"[confirm] Не удалось подтвердить участие в событии {event_id} для {user_id}")
            except Exception as e:
                logger.error(f"[confirm] Ошибка для {user_id}: {e}", exc_info=True)
                await callback.answer("Произошла ошибка при подтверждении.")

    except ValueError as e:
        logger.error(f"[button_handler] Ошибка преобразования ID в числе для {user_id}, data={data}: {e}", exc_info=True)
        await callback.answer("Ошибка обработки данных. Попробуйте снова.")
    except Exception as e:
        logger.exception(f"[button_handler] Неожиданная ошибка для {user_id}: {e}")
        await callback.answer("Произошла ошибка. Попробуйте ещё раз.")


async def next_event(callback: CallbackQuery, state: FSMContext):
    logger.info(f"[next_event] Переход к следующему событию для user_id={callback.from_user.id}")
    data = await state.get_data()
    current_index = data["current_index"]
    new_index = current_index + 1
    await state.update_data(current_index=new_index)
    logger.debug(f"[next_event] Обновлён индекс событий: {new_index}")
    await show_event(callback, state)


async def show_referral(message: Message, bot):
    logger.info(f"[show_referral] Запрос реферальной ссылки от user_id={message.from_user.id}")
    db = bot.db
    user_id = message.from_user.id
    username = message.from_user.username or "user"
    
    # Генерируем реферальный код
    referral_code = hashlib.md5(f"{user_id}{username}".encode()).hexdigest()[:10]
    logger.debug(f"[show_referral] Сгенерирован реферальный код: {referral_code}")
    
    bot_username = (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{referral_code}"
    
    text = (
        "🔗 Ваша реферальная ссылка:\n\n"
        f"{referral_link}\n\n"
        "Пригласите друга!"
    )
    await message.answer(text)
    logger.info(f"[show_referral] Реферальная ссылка отправлена пользователю {user_id}")

# ONLY FRIENDS

async def friend_events(message: Message, bot, state: FSMContext):
    """Команда /friendevents — показывает кнопки для выбора друга."""
    logger.info(f"[friend_events] Запуск команды /friendevents для user_id={message.from_user.id}")
    db = bot.db
    user_id = message.from_user.id

    try:
        friends = db.get_friends(user_id)
        if not friends:
            await message.answer("У вас пока нет друзей в боте.")
            logger.info(f"[friend_events] У пользователя {user_id} нет друзей.")
            return

        keyboard = InlineKeyboardBuilder()
        for friend in friends:
            if isinstance(friend, dict):
                friend_id = friend.get("id")
                name = friend.get("name", f"Друг {friend_id}")
            else:
                continue  # Пропускаем некорректные записи


            keyboard.add(InlineKeyboardButton(
                text=f"{name} (ID: {friend_id})",
                callback_data=f"show_confirmed_events_{friend_id}"
            ))
        keyboard.adjust(1)

        await message.answer(
            "Выберите друга, чтобы посмотреть его подтверждённые мероприятия:",
            reply_markup=keyboard.as_markup()
        )
        logger.info(f"[friend_events] Клавиатура с друзьями отправлена пользователю {user_id}")
    except Exception as e:
        logger.error(f"[friend_events] Ошибка для user_id={user_id}: {e}", exc_info=True)
        await message.answer("Произошла ошибка при получении списка друзей.")


async def handle_show_confirmed_events(callback: CallbackQuery, bot, state: FSMContext):
    """
    Обрабатывает нажатие кнопки с выбором друга для просмотра его подтверждённых мероприятий.
    """
    logger.info(f"[handle_show_confirmed_events] Callback от user_id={callback.from_user.id}, data='{callback.data}'")


    if not callback.data.startswith("show_confirmed_events_"):
        logger.warning(f"[handle_show_confirmed_events] Некорректный callback.data: {callback.data}")
        return

    try:
        friend_id = int(callback.data.split("_")[-1])
        logger.debug(f"[handle_show_confirmed_events] Определён friend_id={friend_id}")
    except (ValueError, IndexError) as e:
        logger.error(f"[handle_show_confirmed_events] Ошибка разбора ID из callback.data='{callback.data}': {e}")
        await callback.answer("Некорректный ID друга.")
        return

    db = bot.db
    user_id = callback.from_user.id

    # Проверка дружбы
    if not db.are_friends(user_id, friend_id):
        logger.warning(f"[handle_show_confirmed_events] user_id={user_id} пытается посмотреть мероприятия недруга friend_id={friend_id}")
        await callback.answer("Этот пользователь не в списке ваших друзей.")
        await callback.message.edit_reply_markup(None)
        return
    else:
        logger.debug(f"[handle_show_confirmed_events] Дружба подтверждена: {user_id} ↔ {friend_id}")

    # Получение событий
    try:
        confirmed_events = db.get_confirmed_future_events(friend_id)
        logger.info(f"[handle_show_confirmed_events] Найдено {len(confirmed_events)} будущих подтверждённых событий для friend_id={friend_id}")
    except Exception as e:
        logger.error(f"[handle_show_confirmed_events] Ошибка при получении событий для friend_id={friend_id}: {e}", exc_info=True)
        await callback.message.edit_text("Произошла ошибка при получении мероприятий. Попробуйте позже.")
        await callback.answer()
        return

    # Обработка пустого результата
    if not confirmed_events:
        logger.info(f"[handle_show_confirmed_events] Нет будущих подтверждённых событий у friend_id={friend_id}")
        await callback.message.edit_text("У друга нет подтверждённых мероприятий на будущее.")
        await callback.answer()
        return

    # Формирование текста
    try:
        text = f"Подтверждённые мероприятия друга (ID: {friend_id}):\n\n"
        for event in confirmed_events:
            title = html.escape(event["title"] or "Без названия")
            start_datetime = format_moscow_time(event["start_datetime"])
            event_url = event["event_url"] or "#"
            city = event["city"] or "Не указан"

            text += (
                f"<b>{title}</b>\n"
                f"📅 {start_datetime}\n"
                f"📍 {city}\n"
                f"<a href='{event_url}'>Подробнее</a>\n\n"
            )
        logger.debug(f"[handle_show_confirmed_events] Сформирован текст с {len(confirmed_events)} событиями")
    except Exception as e:
        logger.error(f"[handle_show_confirmed_events] Ошибка форматирования текста: {e}", exc_info=True)
        await callback.message.edit_text("Ошибка формирования списка мероприятий.")
        await callback.answer()
        return

    # Отправка результата
    try:
        await callback.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=None,
            disable_web_page_preview=False
        )
        await callback.answer()  # Убираем «часы»
        logger.info(f"[handle_show_confirmed_events] Успешно отправлен ответ для user_id={user_id}")
    except Exception as e:
        logger.error(f"[handle_show_confirmed_events] Ошибка отправки сообщения: {e}", exc_info=True)




async def my_friends(message: Message, bot):
    db = bot.db
    user_id = message.from_user.id
    
    friends = db.get_friends(user_id)
    
    if not friends:
        response = "У вас пока нет друзей в системе."
    else:
        response_lines = ["Ваши друзья:"]
        for friend in friends:
            # Проверка: является ли friend словарём
            if isinstance(friend, dict) and 'name' in friend and 'id' in friend:
                response_lines.append(f"• {friend['name']} (ID: {friend['id']})")
            else:
                # Логируем проблему
                print(f"Unexpected friend item: {friend} (type: {type(friend)})")
                response_lines.append(f"• Неизвестный друг (ID: ???)")
        response = "\n".join(response_lines)
    
    await message.answer(
        chat_id=message.chat.id,
        text=response,
        parse_mode="HTML"
    )


async def handle_select_event_for_invite(callback: CallbackQuery, bot, state: FSMContext):
    logger.info(f"[handle_select_event_for_invite] Начало выбора мероприятия для приглашения, user_id={callback.from_user.id}")
    db = bot.db
    user_id = callback.from_user.id

    try:
        # Получаем ID мероприятия из состояния
        data = await state.get_data()
        event_id = data.get("pending_invite_event_id")
        if not event_id:
            logger.error("[handle_select_event_for_invite] Отсутствует event_id в состоянии")
            await callback.answer("Ошибка: мероприятие не выбрано.")
            return

        # Получаем данные мероприятия
        event = db.get_event_by_id(event_id)
        if not event:
            logger.error(f"[handle_select_event_for_invite] Мероприятие не найдено: event_id={event_id}")
            await callback.answer("Мероприятие не найдено.")
            return

        # Получаем список друзей
        friends = db.get_friends(user_id)
        if not friends:
            await callback.message.edit_text("У вас нет друзей в боте, с которыми можно поделиться мероприятием.")
            await callback.answer()
            logger.info(f"[handle_select_event_for_invite] Нет друзей у пользователя {user_id}")
            return

        keyboard = InlineKeyboardBuilder()
        for friend in friends:
            if isinstance(friend, dict):
                friend_id = friend["id"]
                name = friend.get("name", f"Друг {friend_id}")
                keyboard.add(
                    InlineKeyboardButton(
                        text=name,
                        callback_data=f"invite_friend_{friend_id}_{event_id}"
                    )
                )
        keyboard.adjust(1)

        title = html.escape(event["title"] or "Без названия")
        start_dt = format_moscow_time(event["start_datetime"])
        text = (
            f"Выберите друга, чтобы пригласить на мероприятие:\n\n"
            f"<b>{title}</b>\n"
            f"📅 {start_dt}\n"
        )

        await callback.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()
        logger.info(f"[handle_select_event_for_invite] Клавиатура с друзьями отправлена для приглашения на event_id={event_id}")

    except Exception as e:
        logger.error(
            f"[handle_select_event_for_invite] Ошибка для user_id={user_id}, event_id={data.get('pending_invite_event_id')}: {e}",
            exc_info=True
        )
        await callback.message.edit_text("Произошла ошибка при выборе друзей для приглашения.")
        await callback.answer()


async def handle_invite_event(callback: CallbackQuery, bot, state: FSMContext):
    logger.info(f"[handle_invite_event] Обработка приглашения на мероприятие, user_id={callback.from_user.id}, data='{callback.data}'")


    if not callback.data.startswith("invite_friend_"):
        logger.warning(f"[handle_invite_event] Некорректный callback.data: {callback.data}")
        return

    try:
        parts = callback.data.split("_")
        if len(parts) != 4:
            raise ValueError(f"Некорректный формат callback.data: {callback.data}")


        friend_id = int(parts[2])
        event_id = int(parts[3])

        logger.debug(f"[handle_invite_event] Приглашение: friend_id={friend_id}, event_id={event_id}")


        db = bot.db
        user_id = callback.from_user.id

        # Проверяем, что пользователь и друг — друзья
        if not db.are_friends(user_id, friend_id):
            await callback.answer("Этот пользователь не в вашем списке друзей.")
            logger.warning(f"[handle_invite_event] user_id={user_id} пытается пригласить недруга friend_id={friend_id}")
            return

        # Проверяем существование мероприятия
        event = db.get_event_by_id(event_id)
        if not event:
            await callback.answer("Мероприятие не найдено.")
            logger.error(f"[handle_invite_event] Мероприятие event_id={event_id} не найдено")
            return

        # Отправляем приглашение
        invitation_text = (
            f"Вам приглашение от {callback.from_user.full_name}!\n\n"
            f"<b>{html.escape(event['title'] or 'Без названия')}</b>\n"
            f"📅 {format_moscow_time(event['start_datetime'])}\n"
            f"<a href='{event.get('event_url', '#')}>Подробнее</a>\n\n"
            "Хотите присоединиться?"
        )

        invitation_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_invite_{user_id}_{event_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"decline_invite_{user_id}_{event_id}")
            ]
        ])

        try:
            bot_instance = callback.bot
            await bot_instance.send_message(
                chat_id=friend_id,
                text=invitation_text,
                parse_mode="HTML",
                reply_markup=invitation_kb
            )
            await callback.answer("Приглашение отправлено!")
            logger.info(f"[handle_invite_event] Приглашение отправлено friend_id={friend_id} на event_id={event_id}")
        except Exception as send_err:
            logger.error(f"[handle_invite_event] Ошибка отправки сообщения friend_id={friend_id}: {send_err}", exc_info=True)
            await callback.answer("Не удалось отправить приглашение. Возможно, друг не запускал бота.")


    except (ValueError, IndexError) as e:
        logger.error(f"[handle_invite_event] Ошибка разбора callback.data='{callback.data}': {e}")
        await callback.answer("Некорректные данные приглашения.")
    except Exception as e:
        logger.exception(f"[handle_invite_event] Неожиданная ошибка: {e}")
        await callback.answer("Произошла ошибка при отправке приглашения.")


async def handle_accept_invite(callback: CallbackQuery, bot):
    logger.info(f"[handle_accept_invite] Принятие приглашения, user_id={callback.from_user.id}, data='{callback.data}'")


    if not callback.data.startswith("accept_invite_"):
        logger.warning(f"[handle_accept_invite] Некорректный callback.data: {callback.data}")
        return

    try:
        parts = callback.data.split("_")
        if len(parts) != 4:
            raise ValueError(f"Некорректный формат callback.data: {callback.data}")


        sender_id = int(parts[2])
        event_id = int(parts[3])

        logger.debug(f"[handle_accept_invite] Принятие приглашения: sender_id={sender_id}, event_id={event_id}")


        db = bot.db
        user_id = callback.from_user.id

        # Проверяем существование мероприятия
        event = db.get_event_by_id(event_id)
        if not event:
            await callback.answer("Мероприятие не найдено.")
            logger.error(f"[handle_accept_invite] Мероприятие event_id={event_id} не найдено")
            return

        # Подтверждаем участие
        success = db.confirm_event(user_id, event_id)
        if success:
            # Отправляем уведомление инициатору
            try:
                invitation_accepted_text = (
                    f"🎉 {callback.from_user.full_name} принял ваше приглашение на мероприятие:\n\n"
                    f"<b>{html.escape(event['title'] or 'Без названия')}</b>\n"
                    f"📅 {format_moscow_time(event['start_datetime'])}"
                )
                await bot.send_message(
                    chat_id=sender_id,
                    text=invitation_accepted_text,
                    parse_mode="HTML"
                )
                logger.info(f"[handle_accept_invite] Уведомление отправлено sender_id={sender_id}")
            except Exception as send_err:
                logger.error(
                    f"[handle_accept_invite] Ошибка отправки уведомления sender_id={sender_id}: {send_err}",
                    exc_info=True
                )

            await callback.message.edit_text(
                f"Вы подтвердили участие в мероприятии:\n\n"
                f"<b>{html.escape(event['title'] or 'Без названия')}</b>\n"
                f"📅 {format_moscow_time(event['start_datetime'])}",
                parse_mode="HTML",
                reply_markup=None
            )
            await callback.answer("Участие подтверждено! 😊")
            logger.info(f"[handle_accept_invite] Участие подтверждено user_id={user_id} в event_id={event_id}")
        else:
            await callback.answer("Ошибка подтверждения участия. Попробуйте позже.")
            logger.error(f"[handle_accept_invite] Не удалось подтвердить участие user_id={user_id} в event_id={event_id}")


    except (ValueError, IndexError) as e:
        logger.error(f"[handle_accept_invite] Ошибка разбора callback.data='{callback.data}': {e}")
        await callback.answer("Некорректные данные приглашения.")
    except Exception as e:
        logger.exception(f"[handle_accept_invite] Неожиданная ошибка: {e}")
        await callback.answer("Произошла ошибка при подтверждении участия.")


async def handle_decline_invite(callback: CallbackQuery, bot):
    logger.info(f"[handle_decline_invite] Отклонение приглашения, user_id={callback.from_user.id}, data='{callback.data}'")

    if not callback.data.startswith("decline_invite_"):
        logger.warning(f"[handle_decline_invite] Некорректный callback.data: {callback.data}")
        return

    try:
        parts = callback.data.split("_")
        if len(parts) != 4:
            raise ValueError(f"Некорректный формат callback.data: {callback.data}")


        sender_id = int(parts[2])
        event_id = int(parts[3])

        logger.debug(f"[handle_decline_invite] Отклонение приглашения: sender_id={sender_id}, event_id={event_id}")


        db = bot.db
        user_id = callback.from_user.id

        # Проверяем существование мероприятия
        event = db.get_event_by_id(event_id)
        if not event:
            await callback.answer("Мероприятие не найдено.")
            logger.error(f"[handle_decline_invite] Мероприятие event_id={event_id} не найдено")
            return

        # Отправляем уведомление инициатору
        try:
            invitation_declined_text = (
                f"❌ {callback.from_user.full_name} отклонил ваше приглашение на мероприятие:\n\n"
                f"<b>{html.escape(event['title'] or 'Без названия')}</b>\n"
                f"📅 {format_moscow_time(event['start_datetime'])}"
            )
            await bot.send_message(
                chat_id=sender_id,
                text=invitation_declined_text,
                parse_mode="HTML"
            )
            logger.info(f"[handle_decline_invite] Уведомление об отклонении отправлено sender_id={sender_id}")
        except Exception as send_err:
            logger.error(
                f"[handle_decline_invite] Ошибка отправки уведомления sender_id={sender_id}: {send_err}",
                exc_info=True
            )

        await callback.message.edit_text(
            f"Вы отклонили приглашение на мероприятие:\n\n"
            f"<b>{html.escape(event['title'] or 'Без названия')}</b>\n"
            f"📅 {format_moscow_time(event['start_datetime'])}",
            parse_mode="HTML",
            reply_markup=None
        )
        await callback.answer("Приглашение отклонено.")
        logger.info(f"[handle_decline_invite] Приглашение отклонено user_id={user_id} для event_id={event_id}")

    except (ValueError, IndexError) as e:
        logger.error(f"[handle_decline_invite] Ошибка разбора callback.data='{callback.data}': {e}")
        await callback.answer("Некорректные данные приглашения.")
    except Exception as e:
        logger.exception(f"[handle_decline_invite] Неожиданная ошибка: {e}")
        await callback.answer("Произошла ошибка при отклонении приглашения.")

async def show_invite_event(callback: CallbackQuery, state: FSMContext):
    logger.info(f"[show_invite_event] Показ мероприятия для приглашения, user_id={callback.from_user.id}")
    data = await state.get_data()

    # Получаем текущий индекс и список мероприятий
    current_index = data.get("current_invite_index", 0)
    invitable_events = data.get("invitable_events", [])


    if not invitable_events:
        await callback.message.edit_text("Нет мероприятий для приглашения.")
        await callback.answer()
        logger.warning("[show_invite_event] Список invitable_events пуст")
        return

    if current_index >= len(invitable_events):
        await callback.message.edit_text("Больше нет мероприятий для приглашения.")
        await callback.answer()
        logger.info("[show_invite_event] Достигнут конец списка мероприятий")
        return

    event = invitable_events[current_index]
    event_id = event["id"]
    title = html.escape(event.get("title", "Без названия"))
    start_dt = format_moscow_time(event["start_datetime"])
    event_url = event.get("event_url", "#")
    city = event.get("city", "Не указан")


    text = (
        f"<b>Мероприятие для приглашения:</b>\n\n"
        f"{title}\n"
        f"📅 {start_dt}\n"
        f"📍 {city}\n"
        f"<a href='{event_url}'>Подробнее</a>\n\n"
        "Выберите действие:"
    )

    keyboard = InlineKeyboardBuilder()
    keyboard.add(
        InlineKeyboardButton(
            text="➡️ Следующее",
            callback_data=f"next_invite_{current_index + 1}"
        ),
        InlineKeyboardButton(
            text="👥 Выбрать друзей",
            callback_data=f"invite_to_event_{event_id}"
        )
    )
    keyboard.adjust(1, 1)

    try:
        await callback.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard.as_markup(),
            disable_web_page_preview=False
        )
        await callback.answer()  # Убираем «часы»
        logger.info(f"[show_invite_event] Показано мероприятие event_id={event_id}")
    except Exception as e:
        logger.error(f"[show_invite_event] Ошибка при показе мероприятия: {e}", exc_info=True)
        await callback.answer("Ошибка показа мероприятия. Попробуйте снова.")