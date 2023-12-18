"""
модуль обращется к БД, получает данные для предсказания электропотребления
на предстоящие сутки и передает их моделям на вход
"""
import sqlite3
from typing import List, Tuple, Callable, Optional
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

import pandas as pd

from config import path_to_base


def call_predictors(date: datetime,
                    predictors: List[Tuple[Callable, str, str]],
                    ) -> Optional[pd.DataFrame]:
    """
    функция обращется к БД и в случае, если в БД имеются исходные данные для предсказания,
    выкачивает их и передает на моделям. Получает от них предикты, и усредняет.

    На вход требует дату, на которое будет выполнен предикт.
    А также лист кортежей с функциями, которые делают предсказания,
    и их строковые представления
    """
    conn = sqlite3.connect(path_to_base)
    cursor = conn.cursor()
    # получим последнюю дату, на которую в БД имеются записи о потреблении и температуре
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
    if max_datetime_in_db.date() < (date - timedelta(days=1)).date():
        # в случае отстутствия исходных данных:
        print(f'В БД пока нет данных для прогноза на {date.date()}')
        return None

    # получим последнюю дату, на которую в БД имеются записи о выходных/рабочих днях
    query = """
        SELECT
            MAX(t.datetime)
        FROM
            day_off_table AS t
        WHERE
            t.day_off IS NOT NULL;
        """
    cursor.execute(query)
    max_datetime_in_db_str = cursor.fetchone()[0]
    max_datetime_in_db = datetime.strptime(max_datetime_in_db_str, '%Y-%m-%d 00:00:00')
    if max_datetime_in_db.date() < date.date():
        # в случае отстутствия исходных данных:
        print(f'В БД пока нет сведений о выходных/рабочих днях на {date.date()}')
        return None
    query = """
        SELECT
            MAX(t.datetime)
        FROM
            day_off_table AS t
        WHERE
            t.day_off IS NOT NULL;
        """

    # в случае наличия всех данных:
    start_time = (date - relativedelta(years=1)).replace(hour=0,
                                                         minute=0,
                                                         second=0,
                                                         microsecond=0)
    end_time = date.replace(hour=23,
                            minute=0,
                            second=0,
                            microsecond=0)
    # данное время будет передано моделям для подготовки исходных данных
    # в этот момент делается предсказание
    predict_time = (date - timedelta(days=1)).replace(hour=11,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)

    query = f"""
        SELECT
            t.power_true,
            t.temperature,
            t.datetime,
            (CASE WHEN
                day_off_table.day_off = 1
            THEN
                1
            ELSE
                0
            END) AS day_off
        FROM
            consumption_table AS t
            LEFT JOIN day_off_table ON DATE(t.datetime) = DATE(day_off_table.datetime)
        WHERE
            t.datetime >= '{start_time}' AND
            t.datetime <= '{end_time}';
        """
    data = pd.read_sql(sql=query, con=conn)
    conn.close()
    data['datetime'] = pd.to_datetime(data['datetime'], format='%Y-%m-%d %H:%M:%S')

    predicts = pd.DataFrame()
    # просуммируем значения, пришедшие от моделей
    for (predictor_obj, predictor_name, predictor_path) in predictors:
        predicts[predictor_name] = predictor_obj(data=data.copy(),
                                                 date=predict_time,
                                                 model_path=predictor_path)[0]

    return predicts


if __name__ == '__main__':
    from predictors.lgbm.predictor_lgbm import lgbm_model
    from predictors.rnn.predictor_rnn import rnn_model


    print(call_predictors(date=datetime.strptime('2023-05-01', '%Y-%m-%d'),
                          predictors=[
                            (lgbm_model, 'lgbm', r'predictors\lgbm\models\model_v0'),
                            (rnn_model, 'rnn', r'predictors\rnn\models\model_v0'),
                            (rnn_model, 'rnn_v1', r'predictors\rnn\models\model_v1_2023-12-15'),
                        ]))
