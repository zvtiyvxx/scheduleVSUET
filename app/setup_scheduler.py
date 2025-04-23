from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from app.notification import send_notifications
from parser.auto_parser import download_tables
from datetime import datetime, timedelta

# Настройка планировщика APScheduler
def setup(bot):
    scheduler = AsyncIOScheduler()

    # Добавляем задачу: отправка уведомлений каждый день в 19:00
    scheduler.add_job(send_notifications, "cron", hour=19, minute=0, args=[bot], timezone=timezone("Europe/Moscow"))
    scheduler.add_job(download_tables, "cron", hour=1, minute=0, args=[bot], timezone=timezone("Europe/Moscow"))

    # Запускаем планировщик
    scheduler.start()