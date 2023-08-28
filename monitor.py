"""
модуль, выполняющий мониторинг качества предикта моделей
"""
import sqlite3
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

from energy_consumption.config import path_to_base, path_to_monitor_reports


def check_the_quality(check_date: datetime) -> None:
    """
    функция рассчитывает среднюю абсолютную ошибку
    всех сделанных ранее прогнозов по отношению к фактическим
    значениям потребления

    на вход требует месяц периода в формате datetime
    """
    check_date_str = check_date.strftime("%Y-%m")
    start_date = check_date
    end_date = start_date + relativedelta(months=1) - relativedelta(hours=1)

    conn = sqlite3.connect(path_to_base)
    cursor = conn.cursor()
    # определим список уникальных моделей за указанный период
    query = f"""
        SELECT
            DISTINCT model
        FROM
            predict_table AS t
        WHERE
            t.datetime BETWEEN'{start_date}' AND '{end_date}';
    """

    cursor.execute(query)
    unique_models = [model[0] for model in cursor.fetchall()]

    query = f"""
        SELECT
            t.power_true
        FROM
            consumption_table AS t
        WHERE
            t.power_true IS NOT NULL AND
            t.datetime BETWEEN '{start_date}' AND '{end_date}';
    """
    df = pd.read_sql(sql=query, con=conn)

    if unique_models and len(df) > 0:
        # сформируем перечни колонок для следующего динамического запроса
        select_columns_inner = ', ' \
            .join([
                    f"MAX(CASE WHEN model = '{model}' THEN power_pred END) AS {model}"
                    for model
                    in unique_models
                ])
        select_columns_out = ', '.join(unique_models)
        # данный запрос соберет PIVOT table разгруппировав колонку с моделями
        # для этого мы предварительно получили список уникальных моделей
        query = f"""
                SELECT
                    t.datetime,
                    power_true,
                    {select_columns_out}
                FROM
                    consumption_table
                    INNER JOIN (
                        SELECT
                            datetime,
                            {select_columns_inner}
                        FROM
                            predict_table
                        WHERE
                            datetime BETWEEN '{start_date}' AND '{end_date}'
                        GROUP BY
                            datetime
                    ) AS t ON consumption_table.datetime = t.datetime
                WHERE
                    t.datetime BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY
                    t.datetime
            """

        df = pd.read_sql(sql=query, con=conn)
        conn.close()

        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
        df.set_index('datetime', inplace=True)

        with open(f'{path_to_monitor_reports}/report_{check_date_str}.txt',
                  'w',
                  encoding='utf8') as file:
            file.write(f'За период {check_date_str} в БД {len(df)} записей за {len(df)//24} суток')
            file.write('\n')
            for model in unique_models:
                mae = mean_absolute_error(df['power_true'], df[model]).round(1)
                mape = mean_absolute_percentage_error(df['power_true'], df[model]) * 100
                mape = mape.round(1)
                file.write(f'Средняя ошибка модели {model} составляет {mae} ({mape}) МВт (%)\n')

        # все выбранные уникальные даты
        uniq_dates = list(set(df.index.date))
        uniq_dates.sort()

        # нарисуем все предсказанные и реальные значения на одном холсте
        nrows = len(uniq_dates)//7 + bool(len(uniq_dates)%7)
        ncols = 7
        _, axes = plt.subplots(nrows=nrows,
                               ncols=ncols,
                               figsize=(21, 3*len(uniq_dates)//7 + 3))

        for i in range(nrows):
            for j in range(ncols):
                try:
                    if nrows == 1:
                        ax = axes[j]
                    else:
                        ax = axes[i, j]
                    ax.plot(range(24),
                            df[df.index.date == uniq_dates[i*7 + j]]['power_true'],
                            color='blue',
                            label='true')

                    for model in unique_models:
                        ax.plot(range(24),
                                df[df.index.date == uniq_dates[i*7 + j]][model],
                                label=model,
                                alpha=0.6)

                    ax.set_xticks(range(0, 24, 4))
                    ax.set_yticks([])

                    mae = mean_absolute_error(
                        df[df.index.date == uniq_dates[i*7 + j]]['power_true'],
                        df[df.index.date == uniq_dates[i*7 + j]]['ensemble']
                    ).round(1)

                    mape = mean_absolute_percentage_error(
                        df[df.index.date == uniq_dates[i*7 + j]]['power_true'],
                        df[df.index.date == uniq_dates[i*7 + j]]['ensemble']
                    ) * 100
                    mape = mape.round(1)

                    ax.set_title(f'{uniq_dates[i*7 + j]}\nMAE (MAPE): {mae} ({mape})')
                    ax.legend()

                except IndexError:  # отловим ошибку, когда массив закончился
                    if nrows == 1:
                        ax = axes[j]
                    else:
                        ax = axes[i, j]
                    ax.set_axis_off()

        plt.tight_layout()
        plt.savefig(f'{path_to_monitor_reports}/report_{check_date_str}.jpg')

    else:
        with open(f'{path_to_monitor_reports}/report_{check_date_str}.txt',
                  'w',
                  encoding='utf8') as file:
            file.write(f'За период {check_date_str} в БД найдено 0 записей')

    return None


if __name__ == '__main__':
    check_the_quality(check_date = datetime.strptime('2023-05', '%Y-%m'))
