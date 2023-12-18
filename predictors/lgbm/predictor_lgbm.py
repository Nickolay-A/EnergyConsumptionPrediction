"""
модуль вызывает модель lgdm и возвращает
предсказанные значения потребления на следующие сутки. 
"""
import warnings
import re
import pickle
import os
from datetime import datetime
from typing import List

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta


def lgbm_model(data: pd.DataFrame,
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
                  lags     : List[int],
                  y_lags   : List[int],
                  time_freq: List[str],) -> pd.DataFrame:
        """
        Функция, формирующая исходные для предикта от регрессионной модели LGBM.
        На вход требует:

        Датафрейм data, содержащий данные о почасовом потреблении, темпераутре и выходных днях

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

        # присоединим сдвиги во времени для мощности и температуры
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for lag in lags:
                df[f'P_lag_{lag}'] = df['power_true'].shift(lag)
                df[f't_lag_{lag}'] = df['temperature'].shift(lag)

        # присоединим к вектору из данных о потреблении и температуре данные о времени,
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

        # удалим более не нужные колонки
        df.drop(['power_true',
                 'temperature',
                 'day_off',], axis=1, inplace=True)

        for time_component in time_freq:
            df.drop([f'{time_component}_sin',
                     f'{time_component}_cos',], axis=1, inplace=True)

        return df


    # получим вектор исходных данных для предикта
    lags=list(range(180))
    y_lags=list(range(12, 36))
    time_freq = ['hour', 'day_of_year', 'month', 'weekday']
    X = make_data(df=data,
                  lags=lags,
                  y_lags=y_lags,
                  time_freq=time_freq)
    X = pd.DataFrame(X.loc[date]).astype(float).T

    # для разделения данных по группам для каждой из вызываемой модели,
    # определим перечни колонок
    p_lag_regex = r'P_lag_(\d+)'
    t_lag_regex = r't_lag_(\d+)'
    trig_func_regex = r'[a-zA-Z_]+(?:sin|cos)_(\d+)'
    day_off_regex = r'day_off_(\d+)'
    previos_data_regex = r'[a-zA-Z_]+previos_year_(\d+)'

    p_lag_cols = []
    t_lag_cols = []
    trig_func_cols = []
    day_off_cols = []
    previos_data_cols = []

    power_pred = []

    for regex, cols in zip([p_lag_regex,
                            t_lag_regex,
                            trig_func_regex,
                            day_off_regex,
                            previos_data_regex,],
                           [p_lag_cols,
                            t_lag_cols,
                            trig_func_cols,
                            day_off_cols,
                            previos_data_cols,]):
        cols.extend([col_name for col_name in X.columns if re.match(regex, col_name)])

    # последовательно вызовем все модели и передадим им
    # для предсказания соответсвущие части вектора X
    with open(os.path.join(os.getcwd(), model_path, 'model.pickle'), 'rb') as file:
        model = pickle.load(file)

    for lag in y_lags:
        # сформируем фичи
        x_cols = p_lag_cols + t_lag_cols
        x_cols.extend([col_name for col_name in \
            (trig_func_cols + day_off_cols + previos_data_cols) \
                if re.match(f'[a-zA-Z_]+_{lag}', col_name)])
        X_lag = X.loc[:, x_cols]
        # вызовем predict у модели
        power_pred.append(model[lag].predict(X_lag))

    return np.array(power_pred).reshape(1, -1)
