"""
Файл, вызвающий последовательно все модули для выполнения
однократного предсказания на сутки вперед
"""
import argparse
from datetime import datetime, timedelta

from updatedb import update_db_by_data_from_oik
from predict import call_predictors
from predictors.lgbm.predictor_lgbm import lgbm_model
from predictors.rnn.predictor_rnn import rnn_model
from writetodb import write_to_db
from makereport import make_report


parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_args()

if __name__ == '__main__':
    update_db_by_data_from_oik()

    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d')
    else:
        date = (datetime.now() + timedelta(days=1)).replace(hour=0,
                                                            minute=0,
                                                            second=0,
                                                            microsecond=0)

    pred = call_predictors(date=date, predictors=[(lgbm_model, 'lgbm'),
                                                  (rnn_model, 'rnn')])

    if pred is None:
        print(f'Прогноз на {str(date)} сделать невозможно...')
    else:
        write_to_db(data=pred.copy(), date=date)
        make_report(data=pred.copy(), date=date)
        print(f'Прогноз на {str(date)} выполнен, результаты сохранены')
