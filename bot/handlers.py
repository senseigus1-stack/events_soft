from aiogram import F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, KeyboardButton, ReplyKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder


async def start(message: Message):
    db = message.bot.db
    user_id = message.from_user.id

    # Если пользователь уже есть в БД 
    if db.get_user(user_id):
        await message.answer(
            "Привет! Я бот для рекомендаций мероприятий.\n"
            "Команды:\n"
            "/recommend — подборка событий\n"
            "/help — справка",
            reply_markup=ReplyKeyboardMarkup(keyboard=[])  
        )
        return

    #начинаем регистрацию
    user_message = message.text.strip()

    # выбрал ли пользователь город (если это не /start)
    if user_message in ["МСК", "СПБ", "МСК и СПБ"]:
        # Сопоставляем текст с кодом города
        city_mapping = {
            "МСК": 1,
            "СПБ": 2,
            "МСК и СПБ": 3
        }
        selected_city = city_mapping[user_message]

        # Добавляем пользователя в БД с выбранным городом
        try:
            with db.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, city, status_ml, event_history) VALUES (%s, %s, %s, %s)",
                    (user_id, selected_city, "[]", "[]")
                )
            db.conn.commit()
            print(f"Пользователь {user_id} добавлен в БД с городом {selected_city}")

            # Отправляем приветствие и убираем клавиатуру
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
        return

    # Если это /start и город ещё не выбран — показываем клавиатуру
    elif user_message == "/start":
        keyboard = ReplyKeyboardBuilder()
        keyboard.add(
            KeyboardButton(text="МСК"),
            KeyboardButton(text="СПБ"),
            KeyboardButton(text="МСК и СПБ")
        )
        keyboard.adjust(1)  # Одна кнопка в ряду

        await message.answer(
            "Выберите город, от которого будете получать события:",
            reply_markup=keyboard.as_markup(resize_keyboard=True)
        )
    else:
        # Если пользователь написал что-то другое — напоминаем про /start
        await message.answer(
            "Напишите /start, чтобы начать регистрацию.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[])
        )



async def recommend(message: Message, bot):
    db = bot.db
    ml = bot.ml
    user_id = message.from_user.id
    user = db.get_user(user_id)
    
    if not user:
        await message.answer("Сначала напишите /start")
        return
    
    candidates = db.get_recommended_events(table_name='msk', limit=50)
    recommended = ml.recommend(user["event_history"], candidates)
    
    if not recommended:
        await message.answer("Пока нет рекомендаций. Оцените несколько событий!")
        return
    
    for event in recommended:
        keyboard = InlineKeyboardBuilder()
        keyboard.add(
            InlineKeyboardButton(text="👍", callback_data=f"like_{event['id']}"),
            InlineKeyboardButton(text="👎", callback_data=f"dislike_{event['id']}")
        )
        
        text = (
            f"<b>{event['title']}</b>\n"
            f"{event['description'][:200]}...\n"
            f"📅 {event['start_datetime']}\n"
            f"<a href='{event['event_url']}'>Подробнее</a>"
        )
        await message.answer(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")


async def button_handler(callback: CallbackQuery, bot):
    user_id = callback.from_user.id
    data = callback.data
    db = bot.db
    ml = bot.ml
    
    if data.startswith("like_"):
        event_id = int(data.split("_")[1])
        db.add_event_to_history(user_id, event_id, "like")
        
        event = next(
            (e for e in db.get_recommended_events(limit=100) if e["id"] == event_id),
            None
        )
        if event:
            user = db.get_user(user_id)
            new_status_ml = ml.update_user_status_ml(
                user["status_ml"], event["status_ml"], weight=0.3
            )
            db.update_user_status_ml(user_id, new_status_ml)
        
        await callback.answer("Спасибо за оценку! 😊")
        await callback.message.edit_reply_markup(None)
    
    elif data.startswith("dislike_"):
        event_id = int(data.split("_")[1])
        db.add_event_to_history(user_id, event_id, "dislike")
        await callback.answer("Не будем показывать такое. 😐")
        await callback.message.edit_reply_markup(None)