import aiosqlite
from config import db_tg, db_schedules, db_users

async def save_users_schedule(user_id, group, subgroup, table_name):
    try:
        # Подключение к базе данных с расписаниями групп
        async with aiosqlite.connect(db_tg) as schedule_conn:
            schedule_cursor = await schedule_conn.cursor()

            # Извлечение расписания из указанной таблицы расписаний
            await schedule_cursor.execute(f"""
                SELECT day, time, ChZn, para,
                       CASE 
                           WHEN day = 'Понедельник' THEN 1
                           WHEN day = 'Вторник' THEN 2
                           WHEN day = 'Среда' THEN 3
                           WHEN day = 'Четверг' THEN 4
                           WHEN day = 'Пятница' THEN 5
                           WHEN day = 'Суббота' THEN 6
                           WHEN day = 'Воскресенье' THEN 7
                           ELSE 8
                       END AS day_order
                FROM {table_name} 
                WHERE groups = ? AND (subgroup = ? OR subgroup = 0) AND para IS NOT NULL AND para != ''
                ORDER BY day_order, time
            """, (group, subgroup))

            schedule_data = await schedule_cursor.fetchall()

        if not schedule_data:
            print(f"No schedule found for group {group}, subgroup {subgroup} in table {table_name}.")
            return

        # Подключение к базе данных расписаний пользователей
        async with aiosqlite.connect(db_schedules) as user_conn:
            # Создание таблицы, если ее нет
            await user_conn.execute("""
                CREATE TABLE IF NOT EXISTS user_schedules (
                    user_id INTEGER NOT NULL,
                    day TEXT NOT NULL,
                    time TEXT NOT NULL,
                    chzn TEXT NOT NULL,
                    para TEXT NOT NULL
                )
            """)
            await user_conn.commit()

            # Удаление старых записей для пользователя
            await user_conn.execute("DELETE FROM user_schedules WHERE user_id = ?", (user_id,))

            # Сохранение нового расписания
            await user_conn.executemany(
                """
                INSERT INTO user_schedules (user_id, day, time, chzn, para)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(user_id, day, time, chzn, para) for day, time, chzn, para, _ in schedule_data]
            )
            await user_conn.commit()

    except aiosqlite.Error as e:
        print(f"Database error: {e}")

async def send_for_week(bot, user_id, ChZn):
    try:
        # Подключение к базе данных с расписанием пользователей
        async with aiosqlite.connect(db_schedules) as conn:
            cursor = await conn.cursor()

            # Извлечение расписания для указанного пользователя с учетом `Числ/Знамен`
            await cursor.execute(
                """
                SELECT day, time, para
                FROM user_schedules
                WHERE user_id = ? AND (chzn = ? OR chzn = 'Числ/Знамен')
                ORDER BY 
                    CASE 
                        WHEN day = 'Понедельник' THEN 1
                        WHEN day = 'Вторник' THEN 2
                        WHEN day = 'Среда' THEN 3
                        WHEN day = 'Четверг' THEN 4
                        WHEN day = 'Пятница' THEN 5
                        WHEN day = 'Суббота' THEN 6
                        WHEN day = 'Воскресенье' THEN 7
                        ELSE 8
                    END, time
                """,
                (user_id, ChZn)
            )

            schedule = await cursor.fetchall()

        # Проверка, есть ли расписание
        if not schedule:
            await bot.send_message(user_id, f"Расписание для '{ChZn}' недели не найдено.")
            return

        # Формирование текста расписания
        weekly_schedule = {}
        for day, time, para in schedule:
            if day not in weekly_schedule:
                weekly_schedule[day] = []
            weekly_schedule[day].append((time, para))

        # Отправка расписания пользователю
        response = f"🗓 <b>Расписание на неделю ({ChZn}):</b>\n"
        for day, lessons in weekly_schedule.items():
            response += f"\n<b>{day.upper()}</b>\n"
            for idx, (time, para) in enumerate(lessons, start=1):
                response += f"<b>{idx} пара:</b> {time} | {para}\n"

        await bot.send_message(user_id, response, parse_mode="HTML")

    except aiosqlite.Error as e:
        print(f"Database error: {e}")
        await bot.send_message(user_id, "Произошла ошибка при получении расписания.")

async def update_unique_schedules():
    try:
        # Подключение к базе данных пользователей
        async with aiosqlite.connect(db_users) as user_conn:
            cursor = await user_conn.cursor()

            # Извлечение всех пользователей с их группами и подгруппами
            await cursor.execute("SELECT user_id, groups, subgroup, table_name FROM users")
            users_data = await cursor.fetchall()

        if not users_data:
            print("No users found in the database.")
            return

        # Подключение к базе данных с расписаниями групп
        async with aiosqlite.connect(db_tg) as schedule_conn:
            # Для каждого пользователя обновляем расписание
            for user_id, group, subgroup, table_name in users_data:
                await save_users_schedule(user_id, group, subgroup, table_name)

        return f"All user schedules have been updated successfully."

    except aiosqlite.Error as e:
        return f"Database error during update: {e}"


