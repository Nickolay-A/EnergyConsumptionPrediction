"""
Модуль создаёт первоначальную базу данных
"""
import sqlite3

import pandas as pd

from config import path_to_base


if __name__ == '__main__':

    conn = sqlite3.connect(path_to_base)
    cursor = conn.cursor()

    # Создадим таблицы
    cursor.execute("""CREATE TABLE IF NOT EXISTS consumption_table
                          (id         INTEGER PRIMARY KEY AUTOINCREMENT,
                          power_true  REAL,
                          temperature REAL,
                          datetime    TIMESTAMP)""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS day_off_table
                          (id         INTEGER PRIMARY KEY AUTOINCREMENT,
                          day_off     INTEGER,
                          datetime    TIMESTAMP,
                          FOREIGN KEY (datetime) REFERENCES consumption_table (datetime))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS predict_table
                          (id         INTEGER PRIMARY KEY AUTOINCREMENT,
                          power_pred  REAL,
                          model       TEXT,
                          datetime    TIMESTAMP,
                          FOREIGN KEY (datetime) REFERENCES consumption_table (datetime))""")

    # заполним таблицу consumption_table
    df = pd.read_csv('./data/consumption_and_temperature_data.csv')
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
    df.set_index('datetime', inplace=True)
    # расширим таблицу до конца года для совместимости с таблицей day_off_table
    df = pd.concat([df,
                    pd.DataFrame(columns=df.columns,
                                 index=pd.date_range(start='2023-06-01 00:00:00',
                                                     end='2023-12-31 23:00:00',
                                                     freq='H'))])
    # переименуем колонки
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'datetime',
                       'one_hour_consumption': 'power_true',
                       'one_hour_temperature': 'temperature'},
             inplace=True)
    df = df[['power_true', 'temperature', 'datetime']]
    # запишем в БД
    df.to_sql(name='consumption_table',
              con=conn,
              if_exists='append',
              index=False)

    # заполним таблицу day_off_table
    df = pd.read_excel('./data/calendar.xlsx', index_col='day')
    df.reset_index(inplace=True)
    df.rename(columns={'day': 'datetime'}, inplace=True)
    # запишем в БД
    df.to_sql(name='day_off_table',
              con=conn,
              if_exists='append',
              index=False)
    # закроем подключение
    conn.close()
