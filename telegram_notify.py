import os
import traceback
import warnings

import telebot
from luigi import Task

CHAT_ID = '<chat_id>'
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')


def send_notification(task: Task, exception: Exception):
    if BOT_TOKEN is None:
        warnings.warn('TG_BOT_TOKEN is not set')
        # если не задан токен в переменной окружения, то просто выходим, чтобы не ломать
        # работу пайплайна
        return

    bot = telebot.TeleBot(BOT_TOKEN)
    text = (
        f'Упала задача: {task.__class__.__name__}\n'
        f'Исключение: {exception.__class__.__name__}.\n\n{traceback.format_exc()}'
    )
    bot.send_message(CHAT_ID, text)
