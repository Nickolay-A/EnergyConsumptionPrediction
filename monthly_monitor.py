"""
Файл, выполняющий месячный мониторинг качества модели
"""
import os
import argparse
from datetime import datetime

from dateutil.relativedelta import relativedelta

from config import path_to_monitor_reports
from accesstobase import check_base
from predict import call_predictors
from predictors.lgbm.predictor_lgbm import lgbm_model
from predictors.rnn.predictor_rnn import rnn_model
from writetodb import write_to_db
from makereport import make_report
from monitor import check_the_quality
from connect_to_oik import load_data_to_oik


parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_args()

if __name__ == '__main__':

    if args.date:
        date = datetime.strptime(args.date, '%Y-%m')
    else:
        date = (datetime.now() - relativedelta(months=1)).replace(day=1,
                                                                  hour=0,
                                                                  minute=0,
                                                                  second=0,
                                                                  microsecond=0)

    if os.path.isfile(f'{path_to_monitor_reports}/report_{date.strftime("%Y-%m")}.txt'):

        print(f'Мониторинг за {date.strftime("%Y-%m")} выполнялся ранее')

    else:

        dates = check_base(date=date)

        if dates is None:
            print(f'За {date.strftime("%Y-%m")} были выполнены все прогнозы')
        else:
            for date_ in dates['dates']:

                pred = call_predictors(date=date_,
                                       predictors=[
                    # (lgbm_model, 'lgbm'),
                    # (rnn_model, 'rnn'),
                    (rnn_model, 'rnn_v1', r'predictors\rnn\models\model_v1_2023-12-15'),
                ])

                if pred is None:
                    print(f'Прогноз на {str(date_)} сделать невозможно...')
                else:
                    write_to_db(data=pred.copy(), date=date_)
                    make_report(data=pred.copy(), date=date_)
                    print(f'Прогноз на {str(date_)} выполнен, результаты сохранены')
                    load_data_to_oik(date=date_)

        check_the_quality(check_date=date)

        print(f'Выполнен мониторинг за {date.strftime("%Y-%m")}')
