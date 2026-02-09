



# проверить работу, а добавлять в друзтя по нику, а не по ID (сделать завтра)

# add разобраться с добавлением своих мероприятий(*проверка на легальность с интеграцией ГИГАЧАТ)


from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

def format_datetime(timestamp: int) -> str:
    """Форматирует UNIX‑время в читаемый вид (UTC)."""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%d.%m.%Y в %H:%M")

async def send_reminder(bot, db):
    """
    Задача: найти мероприятия через 24 ч и отправить напоминания участникам.
    Использует:
      - get_upcoming_confirmed(days_ahead=1) → события за 1 день;
      - get_event_by_id() → детали мероприятия;
      - mark_reminder_sent() → отметка, что напоминание отправлено.
    """
    try:
        # Получаем предстоящие подтверждённые мероприятия (за 1 день)
        upcoming_events = db.get_upcoming_confirmed(days_ahead=1)
        logger.info(f"Найдены мероприятия за 24 ч: {len(upcoming_events)}")

        for item in upcoming_events:
            user_id = item["user_id"]
            event_id = item["event_id"]
            event_title = item["title"]
            event_url = item["event_url"]

            # Получаем полные данные мероприятия
            # Предполагаем, что события могут быть в таблицах 'msk' или 'spb'
            event = db.get_event_by_id(event_id, "msk")
            if not event:
                event = db.get_event_by_id(event_id, "spb")
            if not event:
                logger.warning(f"Мероприятие {event_id} не найдено ни в msk, ни в spb")
                continue

            # Формируем сообщение
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"🔔 Напоминание!\n\n"
                        f"Мероприятие: *{event['title']}*\n"
                        f"Когда: {format_datetime(int(event['start_datetime']))}\n"
                        f"Ссылка: {event['event_url']}"
                    ),
                    parse_mode="Markdown"
                )
                logger.info(f"Напоминание отправлено пользователю {user_id} для события {event_id}")

                # Отмечаем, что напоминание отправлено
                db.mark_reminder_sent(user_id, event_id)

            except Exception as e:
                logger.error(f"Ошибка отправки напоминания {user_id} → {event_id}: {e}")

    except Exception as e:
        logger.error(f"[send_reminder] Неожиданная ошибка: {e}", exc_info=True)

def setup_scheduler(bot, db):
    """
    Инициализирует планировщик и добавляет задачу.
    Вызывать при старте бота.
    """
    # Задача: каждый день в 09:00 UTC проверяем мероприятия на завтра
    scheduler.add_job(
        send_reminder,
        trigger=CronTrigger(hour=9, minute=0, timezone="UTC"),
        args=[bot, db],
        id="daily_reminder",
        misfire_grace_time=3600,
        max_instances=1
    )
    scheduler.start()
    logger.info("Планировщик запущен: ежедневные напоминания в 09:00 UTC")