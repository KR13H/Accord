from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab


BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

celery = Celery("accord_workers", broker=BROKER_URL, backend=RESULT_BACKEND)
celery.conf.timezone = "Asia/Kolkata"
celery.conf.enable_utc = True
celery.conf.imports = ("workers.rent_tasks", "workers.backup_tasks")
celery.conf.beat_schedule = {
    "generate-monthly-rent-invoices-daily": {
        "task": "workers.rent_tasks.generate_monthly_rent_invoices",
        "schedule": 86400.0,
    },
    "daily-sqlite-backup": {
        "task": "workers.backup_tasks.run_daily_sqlite_backup",
        "schedule": crontab(hour=2, minute=0),
    }
}
