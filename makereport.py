"""
модуль формирует отчёт в формате .xlsx,
а также сохраняет график как картинку в формате .jpg
"""
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from config import path_to_reports


def make_report(data: pd.DataFrame, date: datetime) -> None:
    """
    функция, формирующая отчёт в формате .xlsx,
    а также сохраняет график как картинку в формате .jpg
    """
    dates = [date + timedelta(hours=i) for i in range(24)]
    data['power_pred'] = data.mean(axis=1)
    data.drop(data.columns[:-1], axis=1, inplace=True)
    data.index = dates
    data.to_excel(f'{path_to_reports}/Прогноз потребления электроэнергии на {date.date()}.xlsx')

    _, ax = plt.subplots(figsize=(6, 6))

    ax.plot(range(24), data['power_pred'], label='Прогнозное значение потребления')
    ax.set_xlabel('Время суток')
    ax.set_ylabel('Среднечасовое потребление, МВт')
    ax.set_title(f'Прогноз почасового потребления электроэнергии на {date.date()}')

    plt.grid(True)
    plt.savefig(f'{path_to_reports}/Прогноз потребления электроэнергии на {date.date()}.jpg')

    return None


if __name__ == '__main__':
    df = pd.DataFrame()
    df['lgbm'] = np.random.random(size=(1, 24))[0] * 10000
    df['rnn'] = np.random.random(size=(1, 24))[0] * 10000
    make_report(data=df, date=datetime.strptime('2000-01-01', '%Y-%m-%d'))
