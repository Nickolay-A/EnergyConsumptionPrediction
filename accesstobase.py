"""
модуль, выполнящий обращение к базе данных с целью
определения недостающих данных
"""
import sqlite3
from typing import Optional
from datetime import datetime

import pandas as pd

from config import path_to_base


def check_base(date: datetime) -> Optional[pd.DataFrame]:
    """
    функция обращается к БД с целью определения перечня дней
    в месяце, на которые не был выполнен прогноз

    принимае на вход дату в формате datetime

    Возвращает датафрейм с колонкой dates
    """
    conn = sqlite3.connect(path_to_base)
    # извлечем все имеющиеся даты за указанный месяц,
    # на которые имеются фактические данные, но нет прогнозов
    query = f"""
        SELECT
            t1.date AS dates
        FROM
            (SELECT
                DATE(t1.datetime) AS date
            FROM
                consumption_table AS t1
            WHERE
                SUBSTRING(t1.datetime, 1, 7) = SUBSTRING('{date}', 1, 7)
            GROUP BY
                DATE(t1.datetime)) AS t1
            LEFT JOIN
            (SELECT
                DATE(t2.datetime) AS date
            FROM
                predict_table AS t2
            WHERE
                SUBSTRING(t2.datetime, 1, 7) = SUBSTRING('{date}', 1, 7)
            GROUP BY
                DATE(t2.datetime)) AS t2 ON t1.date = t2.date
        WHERE t2.date IS NULL;
    """

    df = pd.read_sql(sql=query, con=conn)
    df['dates'] = pd.to_datetime(df['dates'], format='%Y-%m-%d')
    conn.close()

    if len(df) > 0:
        return df

    return None


if __name__ == '__main__':

    conn_ = sqlite3.connect(path_to_base)
    cursor_ = conn_.cursor()
    # для тестирования создадим временную таблицу для хранения части предиктов
    query_ = """
        CREATE TABLE
            temp_table AS
        SELECT
            *
        FROM
            predict_table AS t
        WHERE 
            DATE(t.datetime) = '2022-12-31';
    """
    cursor_.execute(query_)
    conn_.commit()
    # удалим эти предикты из основной таблицы
    query_ = """
        DELETE FROM
            predict_table AS t
        WHERE
            DATE(t.datetime) = '2022-12-31'
    """
    cursor_.execute(query_)
    conn_.commit()
    # вызовем тестируемую функцию
    check = check_base(date=datetime.strptime('2022-12', '%Y-%m'))
    print(check)
    # вернем данные на место
    query_ = """
        INSERT INTO
            predict_table (power_pred, model, datetime)
        SELECT
            power_pred,
            model,
            datetime
        FROM
            temp_table;
    """
    cursor_.execute(query_)
    conn_.commit()
    # дропнем временную таблицу
    cursor_.execute("""DROP TABLE IF EXISTS temp_table;""")
    conn_.commit()
    # еще раз вызовем тестируемую функцию
    check = check_base(date=datetime.strptime('2022-12', '%Y-%m'))
    print(check)

    conn_.close()
