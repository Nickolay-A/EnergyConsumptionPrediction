"""
модуль содержит функцию для очитски датафрейма с timeseries от аномалий
"""
import pandas as pd
import numpy as np


def catch_time_series_outs(df: pd.DataFrame,
                           col_name: str,
                           min_diff: int,
                           time_delta: int,
                           freq: str) -> None:
    """
    df - датафейм с временными рядами
    col_name - имя колонки, в которой будем удалять аномалии
    min_diff - минимальное значение разницы двух соседних измерений,
    чтобы назвать его аномалией
    time_delta - максимальное время существования данной аномалии
    freq - временная частота индексов датафрейма
    (смотреть документацию по pandas.date_range)
    Некоторые из допустимых:
    H - hourly frequency
    min - minutely frequency
    S - secondly frequency

    Функция заменяет факты аномалии на np.nan и вызывает метод .ffill() у датафрейма
    """
    df_diff = np.abs(df.diff())
    out_series = df_diff[df_diff[col_name] > min_diff].loc[:,col_name]
    out_times = out_series.index
    out_indexis = pd.Series(dtype='datetime64[ns]')

    time_delta_dict = {'H': 'hour',
                       'min': 'min',
                       'S': 'sec'}

    for i in range(len(out_times) - 1):

        timediff = out_times[i+1] - out_times[i]
        timediff /= pd.Timedelta(value=1,
                                 unit = time_delta_dict[freq])

        if timediff <= time_delta:
            time_range = pd.Series(pd.date_range(start=out_times[i],
                                                end=out_times[i+1],
                                                freq=freq,
                                                inclusive='left'))

            out_indexis = pd.concat([out_indexis, time_range])

    df.loc[out_indexis, col_name] = np.nan
    df.ffill(inplace=True)
