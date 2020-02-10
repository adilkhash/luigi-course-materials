import os
import traceback
import warnings

from luigi import Task
import telebot

CHAT_ID = '<chat_id>'
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')


def send_notification(task: Task, exception: Exception):
    if BOT_TOKEN is None:
        warnings.warn('TG_BOT_TOKEN is not set')
        return

    bot = telebot.TeleBot(BOT_TOKEN)
    text = (
        f'Упала задача: {task.__class__.__name__}\n'
        f'Исключение: {exception.__class__.__name__}, {traceback.format_exc()}'
    )
    bot.send_message(CHAT_ID, text)
