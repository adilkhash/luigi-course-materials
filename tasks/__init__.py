from luigi import Task
from luigi.event import Event

from telegram_notify import send_notification

Task.event_handler(Event.FAILURE)(send_notification)
