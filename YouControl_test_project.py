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
    select* from `youcontrol-test-project.youcontrol_test_dataset.Test`
"""

# Загрузка результату запита в Pandas DataFrame
df = client.query(query).to_dataframe()

#Пошук останьої взаємодії кожного унікальгного користувача з будь яким продуктом компанії
last_date_interaction_with_product = df.groupby('userID') \
    .agg({'Date':'max'}) \
    .reset_index()

# Зведення до типу данних datetime
last_date_interaction_with_product["Date"] = pd.to_datetime(last_date_interaction_with_product["Date"])

#Перевірка чи пройшло 30 днів після останної взаємодії
report_data = last_date_interaction_with_product[datetime.strptime('2023-02-18', '%Y-%m-%d') 
                                                 - last_date_interaction_with_product.Date 
                                                 > pd.Timedelta(days=30)][['userID']].reset_index(drop=True)

# Збереження звіту
report_data.to_excel(f'C:/Users/Андрей/computer/Desktop/YouControl_test/report_data.xlsx', index=False)

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

    await bot.send_message(chat_id=chat_id, text=f"Звіт користувачів, які не користувались продуктами компанії більше 30-ти днів за 2023-02-18, кількість користувачів = {report_data.shape[0]}.")

    with open(file_path, 'rb') as file:
        await bot.send_document(chat_id=chat_id, document=InputFile(file, filename='report_data.xlsx'))

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(send_message_with_file())