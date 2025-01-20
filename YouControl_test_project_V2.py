#Імпорт бібліотек
from google.cloud import bigquery
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns
import pandas as pd

key_path = "C:/Users/Андрей/computer/Desktop/YouControl_test/youcontrol-test-project-de9175ee8095.json"

# Створення клієнта BigQuery
client = bigquery.Client.from_service_account_json(key_path)

query = """
    select userID
    from `youcontrol-test-project.youcontrol_test_dataset.Test`
    group by userID
    having DATE_DIFF(DATE '2023-02-18', max(Date), DAY) > 30
"""

# Загрузка результату запита в Pandas DataFrame
df = client.query(query).to_dataframe()

# Збереження звіту
df.to_excel(f'C:/Users/Андрей/computer/Desktop/YouControl_test/report_data.xlsx', index=False)

import nest_asyncio
import asyncio
from telegram import Bot, InputFile


nest_asyncio.apply()

# токен @BotFather
bot_token = '7696226733:AAFewSSOqYRT7vM85p37I3erKqbKhjxHREU'

# ID чата @userinfobot в Telegram
chat_id = '727013047'

# Шлях до файлу, який відправляємо
file_path = 'C:/Users/Андрей/computer/Desktop/YouControl_test/report_data.xlsx'

async def send_message_with_file():
    bot = Bot(token=bot_token)

    await bot.send_message(chat_id=chat_id, text=f"Звіт користувачів, які не користувались продуктами компанії більше 30-ти днів за 2023-02-18, кількість користувачів = {df.shape[0]}.")

    with open(file_path, 'rb') as file:
        await bot.send_document(chat_id=chat_id, document=InputFile(file, filename='report_data.xlsx'))

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(send_message_with_file())