import asyncio
import logging

from aiogram import Bot,Dispatcher,types
from aiogram.filters import CommandStart, StateFilter, Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from app.keyboards import admin
from app.work_schedulesdb import save_users_schedule, send_for_week, update_unique_schedules
from app.setup_scheduler import setup
from app import keyboards
from config import BOT_TOKEN
from config import ADMIN_ID
from dataBase.database import check_user_group, add_user_info, save_notification_state, get_all_user_ids
from parser.manual_parse import parsing_from_bd, clear_bd, folder


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

class Reg(StatesGroup):
    group = State()
    subgroup = State()

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await message.reply("Введите вашу группу (Например У-232)")
    await state.set_state(Reg.group)

@dp.message(StateFilter(Reg.group))
async def handle_group(message: Message, state: FSMContext):
    groups = message.text.strip()
    user_id = message.from_user.id
    results = await check_user_group(groups)

    if results['group_exists']:
        if results['has_subgroup']:
            await state.update_data(group = groups, file_name = results['table_name'])
            await message.reply("Введите вашу подгруппу (1 или 2)")
            await state.set_state(Reg.subgroup)
        else:
            await add_user_info(user_id, groups, 1, results['table_name'], 11)
            await save_users_schedule(user_id, groups, 1, results['table_name'])
            await message.reply(f"Группа {groups} успешно сохранена", reply_markup=keyboards.main_button)
            await state.clear()
    else:
        await message.reply("Группа не найдена, попробуйте снова", reply_markup=keyboards.main_button)
        await state.clear()

@dp.message(StateFilter(Reg.subgroup))
async def handle_subgroup(message: Message, state: FSMContext):
    subgroup = message.text.strip()
    user_id = message.from_user.id
    if subgroup not in ["1", "2"]:
        await message.reply("Неверная подгруппа. Введите 1 или 2.")
        return

    data = await state.get_data()
    groups = data.get('group')
    file_name = data.get('file_name')

    await add_user_info(user_id, groups, int(subgroup), file_name, 11)
    await save_users_schedule(user_id, groups, int(subgroup), file_name)
    await message.reply(
        f"Ваша группа {groups} и подгруппа {subgroup} успешно сохранены.",
        reply_markup=keyboards.main_button
    )
    await state.clear()

@dp.message(Command(commands=["broadcast"]))
async def broadcast_message(message: Message):
    if message.from_user.id not in ADMIN_ID:
        return await message.reply("У вас нет доступа к этой команде.")

    if not message.reply_to_message:
        return await message.reply("Сделай реплай на сообщение, которое нужно разослать.")

    await message.reply("Начинаю рассылку...")

    user_ids = await get_all_user_ids()
    sent = 0
    total = len(user_ids)

    for i, user_id in enumerate(user_ids, start=1):
        try:
            await bot.forward_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.reply_to_message.message_id
            )
            sent += 1
        except Exception as e:
            logging.warning(f"[!] Не удалось отправить {user_id}: {e}")
        await asyncio.sleep(10)

    await message.answer(f"Рассылка завершена. Успешно отправлено {sent} из {total}.")

@dp.message(Command(commands=['admin']))
async def admin_mode(message: types.Message, state: FSMContext):
    if message.from_user.id in ADMIN_ID:
        await message.answer(f"Добро пожаловать!\nПеред загрузкой расписаний обновите папку {folder}", reply_markup=keyboards.admin)
    else:
        await message.reply("У вас нет прав для доступа к админ-панели.")

@dp.message(lambda message: message.text and message.from_user.id in ADMIN_ID and message.text.lower() in ["спарсить расписания(из папки(из таблиц)) в бд", "удалить бд", "обновить расписания всех пользователей", "выйти из админки"])
async def admin(message: types.Message):
    msg = message.text.strip().lower()
    if msg == "спарсить расписания(из папки(из таблиц)) в бд":
        chat_id = message.chat.id
        await parsing_from_bd(bot, chat_id)
    elif msg == "удалить бд":
        result = await clear_bd()
        await message.reply(result, reply_markup=keyboards.admin)
    elif msg == "обновить расписания всех пользователей":
        result = await update_unique_schedules()
        await message.reply(result, reply_markup=keyboards.admin)
    elif msg == "выйти из админки":
        await message.reply("Теперь ты простой пользователь", reply_markup=keyboards.main_button)
    else:
        await message.reply("Команда не распознана", reply_markup=keyboards.admin)

#@dp.message(lambda message: message.text.lower() == "изменить группу")
@dp.message(lambda message: getattr(message.text, "lower", lambda: None)() == "изменить группу")
async def change_group(message: types.Message, state: FSMContext):
    await message.reply("Введите вашу новую группу (Например: У-232):")
    await state.set_state(Reg.group)

@dp.message(lambda message: message.text.lower() in ["числитель", "знаменатель"])
async def handle_schedule_type(message: Message):
    ChZn = "Числитель" if message.text.lower() == "числитель" else "Знаменатель"
    await send_for_week(bot, message.from_user.id, ChZn)


@dp.message()
async def actions(message: types.Message):
    msg = message.text.strip().lower()
    user_id = message.from_user.id

    if msg == "уведомления":
        await message.answer("Меню уведомлений:", reply_markup=keyboards.notification_button)
    elif msg == "расписание на неделю":
        await message.answer("Выберите вариант:", reply_markup=keyboards.schedule_type_buttons)
    elif msg == "назад":
        await message.answer("Вернулись в главное меню.", reply_markup=keyboards.main_button)
    elif msg == "включить":
        await save_notification_state(user_id, "1")
        await message.reply("Уведомления включены.", reply_markup=keyboards.main_button)
    elif msg == "выключить":
        await save_notification_state(user_id, "0")
        await message.reply("Уведомления выключены.", reply_markup=keyboards.main_button)
    elif msg == "как работает?":
        await message.reply(
            "Уведомления отправляют расписание на следующий день каждый вечер в 19:00, если они включены. ",
            reply_markup=keyboards.notification_button
        )
    else:
        await message.reply("Команда не распознана. Попробуйте снова.")


async def main():
    try:
        async with bot:
            setup(bot)
            logger.info("Бот запущен и готов принимать команды.")
            await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка в работе бота: {e}")

if __name__ == '__main__':
    asyncio.run(main())
