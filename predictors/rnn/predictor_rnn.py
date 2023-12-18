"""
модуль вызывает модель нейронной сети и возвращает
предсказанные значения потребления на следующие сутки. 
"""
import warnings
import re
import os
import pickle
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import numpy as np
import onnxruntime as ort
from dateutil.relativedelta import relativedelta


def rnn_model(data: pd.DataFrame,
              date: datetime,
              model_path: str) -> np.array:
    """
    Функция делает предсказание потребления элетроэнергии на предстоящие сутки вперед
    На вход необходимо передать датафрейм с данными за год
    момент времени, из которого делается предикт
    11:00 соответствует полудню... Вот такие пироги...
    путь к акутальной версии модели
    """
    def make_data(df       : pd.DataFrame,
                  y_lags   : List[int],
                  time_freq: List[str],) -> pd.DataFrame:
        """
        Функция, формирующая исходные для предикта от рекурренной нейронной сети
        На вход требует:

        Датафрейм df, содержащий данные о почасовом потреблении и темпераутре.

        Список лагов, по которым будет осуществляться сдвиг во времени назад.
        Рекомендуется давать начиная с нуля. Последнее значение не включается.

        Список лагов для целевой переменной,
        по которым будет осуществляться сдвиг во времени вперед.
        Рекомендуется давать начиная с единицы. Последнее значение не включается.

        Список частот, по которым необходимо кодировать время и дату.
        Доступны: {'hour', 'day_of_year', 'month', 'weekday'}
        """
        time_dict = {'hour'        : 24,
                     'day_of_year' : 365.25,
                     'month'       : 12,
                     'weekday'     : 7}
        df.set_index('datetime', inplace=True)

        # следующеие фичи нужны для рекуррентного входа

        # извлечем тригонометрическую фичу из времени
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for time_component in time_freq:
                df[time_component] = getattr(df.index, time_component)
                df[f'{time_component}_sin'] = \
                    np.sin( 2 * np.pi * df[time_component] / time_dict[time_component])
                df[f'{time_component}_cos'] = \
                    np.cos( 2 * np.pi * df[time_component] / time_dict[time_component])
                df.drop([time_component], axis=1, inplace=True)

        # следующеие фичи нужны для полносвязного входа

        # присоединим к полносвязному вектору данные о времени,
        # назначенном для конкретного предсказания, а также инфу о рабочем/выходном дне
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for lag in y_lags:
                for time_component in time_freq:
                    df[f'{time_component}_sin_{lag}'] = df[f'{time_component}_sin'].shift(-lag - 1)
                    df[f'{time_component}_cos_{lag}'] = df[f'{time_component}_cos'].shift(-lag - 1)
                df[f'day_off_{lag}'] = df['day_off'].shift(-lag - 1)

        # присоединим к вектору данные о прошлогоднем потреблении и температуре в этот же час,
        # а также сведения о выходном/праздничном дне
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            # создадим временный датафрейм, расширенный на год назад,
            # чтобы получить временные сдвиги
            temp_df = pd.concat([pd.DataFrame(columns=df.columns,
                                              index = pd.date_range(start=df.index[0] - \
                                                                    relativedelta(years=1),
                                                                    end=df.index[0],
                                                                    freq='H')[:-1]),
                                 df[['power_true',
                                     'temperature',
                                     'day_off']].copy()
                                ], axis=0)

            for lag in y_lags:
                # получим соответствующие лагу индексы
                shifted_index = tuple(index - \
                                      relativedelta(years=1) + \
                                        relativedelta(hours=lag + 1) for index in df.index)
                # получим значения по временным сдвигам
                df[[f'one_hour_consumption_previos_year_{lag}',
                    f'one_hour_temperature_previos_year_{lag}',
                    f'day_off_previos_year_{lag}']] = \
                        temp_df.loc[pd.DatetimeIndex(shifted_index),
                                    ['power_true',
                                     'temperature',
                                     'day_off']].values

        df.rename(columns={'power_true': 'one_hour_consumption',
                           'temperature': 'one_hour_temperature'},
                  inplace=True)

        return df


    # получим исходные данные для предикта
    y_lags=list(range(12, 36))
    window_size = 179
    time_freq = ['hour', 'day_of_year', 'month', 'weekday']
    X = make_data(df=data,
                  y_lags=y_lags,
                  time_freq=time_freq)

    # для дальнейшей обработки определим перечни колонок

    # рекуррентный вход
    # скаллируемые колонки
    RNN_input_scalled_cols = ['one_hour_consumption', 'one_hour_temperature']

    # нескаллируемые колонки
    RNN_input_not_scalled_regex = r'[a-zA-Z_]+(?:sin|cos|off)\b'
    RNN_input_not_scalled_cols = []

    # полносвязный вход
    # скаллируемые колонки
    Dense_input_scalled_regex = \
        r'(?:one_hour_consumption_previos_year|one_hour_temperature_previos_year)_(\d+)'
    Dense_input_scalled_cols = []

    # нескаллируемые колонки
    Dense_input_not_scalled_regex = \
        r'[a-zA-Z_]+[(?:sin|cos|off)|(?:day_off_previos_year)]_(?=.*\d+)'
    Dense_input_not_scalled_cols = []

    for regex, cols in zip([RNN_input_not_scalled_regex,
                            Dense_input_scalled_regex,
                            Dense_input_not_scalled_regex,],
                           [RNN_input_not_scalled_cols,
                            Dense_input_scalled_cols,
                            Dense_input_not_scalled_cols,]):
        cols.extend([col_name for col_name in X.columns if re.findall(regex, col_name)])

    Dense_input_not_scalled_cols = \
        [item for item in Dense_input_not_scalled_cols if item not in Dense_input_scalled_cols]

    RNN_input_cols = RNN_input_scalled_cols + RNN_input_not_scalled_cols
    Dense_input_cols = Dense_input_scalled_cols + Dense_input_not_scalled_cols

    # выделим из полученных данных матрицу для рекуррентного слоя и
    # вектор для полносвязного слоя
    RNN_input = X.loc[date - timedelta(hours=window_size): date, RNN_input_cols]
    Dense_input = pd.DataFrame(X.loc[date, Dense_input_cols]).astype(float).T

    # загрузим модель и скеллеры для обработки данных
    session = ort.InferenceSession(os.path.join(os.getcwd(), model_path, 'model.onnx'))

    with open(os.path.join(os.getcwd(), model_path, 'scallers.pickle'), 'rb') as file:
        scaller_RNN, scaller_Dense = pickle.load(file)

    # проскаллируем данные
    RNN_input.loc[:, RNN_input_scalled_cols] = scaller_RNN \
        .transform(RNN_input.loc[:, RNN_input_scalled_cols] \
                   .values.reshape(-1, len(RNN_input_scalled_cols)))
    Dense_input.loc[:, Dense_input_scalled_cols] = scaller_Dense \
        .transform(Dense_input.loc[:, Dense_input_scalled_cols] \
                   .values.reshape(-1, len(Dense_input_scalled_cols)))

    # приведем данные к типу np.array
    RNN_input = np.array(RNN_input).reshape(-1, *RNN_input.shape)
    Dense_input = np.array(Dense_input).reshape(Dense_input.shape)
    Input = [RNN_input, Dense_input]

    # вызовем predict у модели
    inputDetails = session.get_inputs()
    pred = session.run(None, {
        inputDetails[0].name: Input[0].astype(np.float32),
        inputDetails[1].name: Input[1].astype(np.float32),
    })

    return np.array(pred).reshape(1, -1)
