import aiosqlite
from datetime import datetime, timedelta
from config import db_users, db_schedules


def get_next_day_name():
    days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    next_day = datetime.now() + timedelta(days=1)
    return days[next_day.weekday()]


def get_week_part():
    today = datetime.now()
    week_number = today.isocalendar()[1]

    if today.weekday() == 6:
        week_number += 1

    return "Числитель" if week_number % 2 == 0 else "Знаменатель"

async def send_notifications(bot):
    next_day = get_next_day_name()
    week_part = get_week_part()  

    async with aiosqlite.connect(db_users) as users_db:
        cursor = await users_db.execute("""
            SELECT user_id FROM users WHERE notification_state = 1
        """)
        users = await cursor.fetchall()
        await cursor.close()

    if not users:
        print("Нет пользователей с включенными уведомлениями")
        return

    async with aiosqlite.connect(db_schedules) as schedules_db:
        for user in users:
            user_id = user[0]

            cursor = await schedules_db.execute("""
                SELECT time, chzn, para FROM user_schedules
                WHERE user_id = ? AND day = ? AND (chzn = "Числ/Знамен" OR chzn = ?)
                ORDER BY time
            """, (user_id, next_day, week_part))
            schedule = await cursor.fetchall()
            await cursor.close()

            if schedule:
                schedule_text = f"<b>🔔Ваше расписание на завтра ({next_day} | {week_part}):</b>\n\n"
                for idx, entry in enumerate (schedule, start = 1):
                    time, chzn, para = entry
                    schedule_text += f"<b>{idx} пара:</b> {time} | {para}\n"

                try:
                    await bot.send_message(chat_id=user_id, text=schedule_text, parse_mode="HTML")
                except Exception as e:
                    print(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
            else:
                try:
                    message = "🔔<b>ЗАВТРА НЕТ ПАР</b>"
                    await bot.send_message(user_id, message, parse_mode="HTML")
                    await bot.send_sticker(user_id,
                                           sticker="CAACAgIAAxkBAALhwmc2mEUZQFM6aIUNq8Stvo5VBzNsAAIoEgACSpfRS1V8PHkKHjrGNgQ")
                except Exception as e:
                    print(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
