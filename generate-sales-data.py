#Имитация данных о покупках и продажах акций за прошлый день.
# Данные сохраняются в формате CSV.


from datetime import datetime, timedelta
import pandas as pd
import configparser
from random import randint 

# создаем список акций
config = configparser.ConfigParser() # создаем объект для чтения конфигурационного файла
config.read('config.ini')
COMPANIES = eval(config['Companies']['COMPANIES'])


#получаем текущую дату
today = datetime.today() 
yesterday = today - timedelta(days=1)

if 1 <= today.weekday() <= 5: 
    #  Генерируем данные за вчерашний день.
    
    d = {
            'dt': [yesterday.strftime('%Y-%m-%d') ] * len(COMPANIES)*2,
            'company': COMPANIES * 2,
            'transaction_type': ['buy'] * len(COMPANIES) + ['sell'] * len(COMPANIES),
            'amount': [randint (0, 1000) for _ in range(len(COMPANIES)*2)]
            
        }

df = pd.DataFrame(d)
df.to_csv('sales-data.csv', index=False)

