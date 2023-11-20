"""
Скрипт для обновления базы данных по телеметрии.
Обновления производится при вызове скрипта для составления
прогноза на сутки вперед. База обновляется от последнего имеющегося значения
и до последнего полуденного.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta

import pandas as pd

from config import path_to_base
from connect_to_oik import load_data_from_oik
from preprocessing.timeseriesoutlier import catch_time_series_outs


def update_db_by_data_from_oik() -> None:
    """
    функция проверяет базу данных на наличие самых свежих данных из ОИК,
    в случае необходимости обновления она дописывает данные,
    вызвав функцию connect_to_oik
    """
    conn = sqlite3.connect(path_to_base)
    cursor = conn.cursor()
    # получение текущего времени
    current_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    # определение заполеннности БД свежими данными
    query = """
        SELECT
            MAX(t.datetime)
        FROM
            consumption_table AS t
        WHERE
            t.power_true IS NOT NULL;
        """
    cursor.execute(query)
    max_datetime_in_db_str = cursor.fetchone()[0]
    max_datetime_in_db = datetime.strptime(max_datetime_in_db_str, '%Y-%m-%d %H:%M:%S')
    # определяемся, какие данные необходимо долить
    if current_date.hour >= 12:
        if current_date.date() > max_datetime_in_db.date():
            print('БД будет обновлена до 12 часов этого дня')
            begin_time = max_datetime_in_db + timedelta(hours=1)
            end_time = current_date.replace(hour=12,
                                            minute=0,
                                            second=0,
                                            microsecond=0)
            # здесь из-за того, что мы отнимаем у времени час, а в БД уже есть данные за указанный
            # час, нужно бы добавить два часа, а не один.
            # Тогда мы бы получили как раз только то, что нам нужно
            # А так у нас будет наслоение данных в первой же строке таблицы data.
            # Это нужно для корректной работы функции catch_time_series_outs,
            # затем мы обрежем первую строку
        else:
            print('БД не требует обновления')
            return None
    else:
        current_date = current_date.replace(hour=12,
                                            minute=0,
                                            second=0,
                                            microsecond=0)
        current_date -= timedelta(days=1)
        if current_date.date() > max_datetime_in_db.date():
            print('БД будет обновлена до 12 часов вчерашнего дня')
            begin_time = max_datetime_in_db + timedelta(hours=1)
            end_time = current_date.replace(hour=12,
                                            minute=0,
                                            second=0,
                                            microsecond=0)
        else:
            print('БД не требует обновления')
            return None
    with tempfile.TemporaryDirectory() as temp_dir:
        data = load_data_from_oik(begin_time=begin_time,
                                  end_time=end_time,
                                  file_name=temp_dir+r'\data_from_oik')
    if data is None:
        print('ОИК не вернул данных')
        return None
    data['datetime'] = pd.to_datetime(data['datetime'], format='%d.%m.%Y %H:%M:%S')
    data['datetime'] -= timedelta(hours=1)  # для совместимости времени...
    data.rename(columns={'average_hourly_consumption': 'power_true',
                         'average_hourly_temperature': 'temperature',},
                inplace=True)
    # перед заливкой данных в БД, очистим выбросы
    catch_time_series_outs(df=data,
                           col_name='power_true',
                           min_diff=650,
                           time_delta=1,
                           freq='H')
    catch_time_series_outs(df=data,
                           col_name='temperature',
                           min_diff=10,
                           time_delta=1,
                           freq='H')
    # избавляемся от первой строки, чтобы не было пересечения данных
    data = data.iloc[1:]
    # зальем данные в БД
    # для этого создадим временную таблицу, а потом обновим по ней основую
    data.to_sql(name='temp_table',
                con=conn,
                if_exists='replace',
                index=False)

    query = """
        UPDATE
            consumption_table
        SET
            power_true = (
                SELECT
                    power_true
                FROM
                    temp_table
                WHERE
                    temp_table.datetime = consumption_table.datetime
            ),
            temperature = (
                SELECT
                    temperature
                FROM
                    temp_table
                WHERE
                    temp_table.datetime = consumption_table.datetime
            )
        WHERE
            power_true IS NULL 
            OR temperature IS NULL;
        """
    cursor.execute(query)

    # удалим временную таблицу
    cursor.execute("""DROP TABLE IF EXISTS temp_table;""")

    conn.commit()
    conn.close()
    return None


if __name__ == '__main__':
    update_db_by_data_from_oik()
