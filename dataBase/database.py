import aiosqlite
from config import db_tg, db_users


async def add_user_info(user_id: int, groups: str, subgroup: int, table_name: str, state):
    async with aiosqlite.connect(db_users) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE, 
                groups TEXT, 
                subgroup INTEGER,
                table_name TEXT,
                notification_state INTEGER NOT NULL DEFAULT 0
            )
        """)
        if state == 11:
            cursor = await db.execute("SELECT notification_state FROM users WHERE user_id = ?", (user_id,))
            result = await cursor.fetchone()
            if result is not None:
                state = result[0]
            else:
                state = 0

        await db.execute("""
            INSERT INTO users (user_id, groups, subgroup, table_name, notification_state)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                groups = excluded.groups,
                subgroup = excluded.subgroup,
                table_name = excluded.table_name,
                notification_state = excluded.notification_state
        """, (user_id, groups, subgroup, table_name, state))

        await db.commit()

async def check_user_group(group_name: str) -> dict:

    async with aiosqlite.connect(db_tg) as db:
        query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
        cursor = await db.execute(query_tables)
        tables = [row[0] for row in await cursor.fetchall()]
        await cursor.close()

        for table in tables:
            query_group = f"SELECT EXISTS(SELECT 1 FROM {table} WHERE groups = ?) AS group_exists;"
            cursor = await db.execute(query_group, (group_name,))
            group_exists = (await cursor.fetchone())[0] == 1
            await cursor.close()

            if group_exists:
                query_subgroup = f"""
                    SELECT EXISTS(
                        SELECT 1 FROM {table} 
                        WHERE groups = ? AND subgroup = 2
                    ) AS has_subgroup;
                """
                cursor = await db.execute(query_subgroup, (group_name,))
                has_subgroup = (await cursor.fetchone())[0] == 1
                await cursor.close()

                return {
                    "group_exists": True,
                    "has_subgroup": has_subgroup,
                    "table_name": table,
                }

        return {
            "group_exists": False,
            "has_subgroup": False,
            "table_name": "",
        }

async def save_notification_state(user_id, state):
    async with aiosqlite.connect(db_users) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                groups TEXT,
                subgroup INTEGER,
                table_name TEXT,
                notification_state INTEGER NOT NULL DEFAULT 0
            )
        """)

        await db.execute("""
            UPDATE users
            SET notification_state = ?
            WHERE user_id = ?
        """, (state, user_id))

        await db.commit()

async def get_all_user_ids():
    user_ids = []
    async with aiosqlite.connect(db_users) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            async for row in cursor:
                user_ids.append(row[0])
    return user_ids
