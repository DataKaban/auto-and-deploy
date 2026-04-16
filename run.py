# Скрипт обработки продаж.
# Cгенерированный файл продаж который генерируется файлом generate-sales-data.py будем обрабатывать  и заливать в базу данных на сервере.

import os
import pandas as pd
import configparser
from datetime import datetime, timedelta
import time
import yfinance as yf
import requests

from pgdb import PGDatabase

dirname = os.path.dirname(__file__)


config = configparser.ConfigParser() # создаем объект для чтения конфигурационного файла
config.read(os.path.join(dirname, 'config.ini'))
SALES_PATH = config['Files']['SALES_PATH']
COMPANIES = eval(config['Companies']['COMPANIES'])
DATABASE_CREDS = config['Database']

#проверяем существует ли файл с данными о продажах, если да, то обрабатываем его и загружаем в базу данных, если нет, то выводим сообщение об ошибке.
full_sales_path = os.path.join(dirname, SALES_PATH)
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(full_sales_path)    
    print('Данные о продажах успешно загружены:')
    os.remove(full_sales_path) # удаляем файл после обработки, чтобы не обрабатывать его повторно при следующем запуске. 
    print ("Файл удален после обработки.")
else: 
    sales_df = pd.DataFrame() # создаем пустой датафрейм, чтобы не возникало ошибок при попытке обработки несуществующего файла.    
    print('Файл с данными о продажах не найден.')

# Загрузка данных из yahoo finance.

historical_d = {}
PROXY_URL = "socks5://192.168.1.122:1080" # Прописываем прокси Psiphon

for company in COMPANIES:
    historical_d[company] = yf.download(
        company,
        start=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        end=datetime.today().strftime("%Y-%m-%d"),
        timeout=10
    ).reset_index()
    print(historical_d[company])


# Загрузка данных о продажах в базу данных PostgreSQL    
database = PGDatabase(
    host= DATABASE_CREDS['HOST'],
    port= DATABASE_CREDS['PORT'],
    database= DATABASE_CREDS['DATABASE'],
    user= DATABASE_CREDS['USER'],
    password= DATABASE_CREDS['PASSWORD']
    
)
for i, row in sales_df.iterrows():
    query = f"insert into sales values ('{row['dt']}', '{row['company']}', '{row['transaction_type']}', {row['amount']})" # Removed float() cast
    database.post(query, None) # Changed commit=True to commit=False


# Загрузка данных о котировках в базу данных PostgreSQL
for company, data in historical_d.items():
    data = data.reset_index()
    data.columns = data.columns.get_level_values(0)
    for i, row in data.iterrows():
        query = f"insert into stock (dt, company, open, close) Values (%s, %s, %s, %s)"
        
        values = (
            row['Date'],
            company,
            row['Open'],
            row['Close']
        )
        database.post(
            query,
            values
        )
