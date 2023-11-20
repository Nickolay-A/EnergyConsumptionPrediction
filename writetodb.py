"""
модуль предназначен для записи спрогнозированных значений в БД
"""
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from config import path_to_base


def write_to_db(data: pd.DataFrame, date: datetime) -> None:
    """
    функция записывает в ЛБД значения прогнознозного потребления
    используя дату как ключ

    принимае на вход значения в формате np.array и дату в формате datetime
    """
    assert data.shape[0] == 24, 'Неверный входной формат данных'

    conn = sqlite3.connect(path_to_base)
    cursor = conn.cursor()

    dates = [date + timedelta(hours=i) for i in range(24)]

    data['ensemble'] = data.mean(axis=1)

    cols = data.columns

    for col in cols:
        query = f"""
            SELECT
                t.power_pred
            FROM
                predict_table AS t
            WHERE
                t.datetime = '{dates[0]}' AND
                t.model = '{col}'
            """

        df = pd.read_sql(sql=query, con=conn)

        if len(df) == 0:
            query = """
                INSERT INTO
                    predict_table (power_pred, datetime, model)
                VALUES
                    (?, ?, ?)
            """
            data_values = [(str(date), value) for date, value in zip(dates, data[col])]
            cursor.executemany(query, [(value, date, col) for date, value in data_values])
        else:
            print(f'В БД на {date} имеется проноз от модели {col}')

    conn.commit()
    conn.close()

    print(f'Прогноз на {str(date)} внесен в БД')

    return None


if __name__ == '__main__':
    import numpy as np


    df_ = pd.DataFrame()
    df_['lgbm'] = np.random.random(size=(1, 24))[0]
    df_['rnn'] = np.random.random(size=(1, 24))[0]

    write_to_db(data=df_, date=datetime.strptime('2017-01-01', '%Y-%m-%d'))

    conn_ = sqlite3.connect(path_to_base)
    cursor_ = conn_.cursor()

    query_ = """
        SELECT
            *
        FROM
            predict_table AS t
        WHERE
            DATE(t.datetime) = '2017-01-01'
    """

    sql_df = pd.read_sql(sql=query_, con=conn_)
    print(sql_df)

    query_ = """
        DELETE FROM
            predict_table AS t
        WHERE
            DATE(t.datetime) = '2017-01-01'
    """
    cursor_.execute(query_)

    conn_.commit()
    conn_.close()
