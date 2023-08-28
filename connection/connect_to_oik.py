"""
Модуль для подключения к БД ОИК и получения данных телеизмерений
"""
import os
from datetime import datetime
from typing import Tuple, Optional

import pandas as pd
import win32com.client as win32


class MyCastomException(Exception):
    """
    Кастомное исключение, возникающее при неудачном запуске макроса
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def load_data_from_oik(begin_time : datetime,
                       end_time   : datetime,
                       resolution : str = 'h',
                       file_name  : str = 'data_from_oik',
                       ti_name    : Tuple[str] = ('Л1884', 'Л2023'),
                       ti_alias   : Tuple[str] = ('average_hourly_consumption',
                                                  'average_hourly_temperature')
                       ) -> Optional[pd.DataFrame]:
    r"""
    Функция для вызова .xlsm файла со скриптом. Обращается к ОИК и формирует файл
    со значениями ТИ за указанное время. Возвращает датафрейм с колонками мощности и
    темперауры.
    Требуется передать время начала и конца в формате datetime.datetime
    Возможные варианты временной резолюции:
    n - 1 минута
    h - 1 час
    d - 1 сутки
    Имя файла file_name не должно содержать расширения,
    может содержать относительный путь папки для сохранения
    по следующему образцу: r'data_for_predict\data_from_oik'
    Принимает список ТИ и их наименований для формирования файла
    """
    assert end_time >= begin_time, \
        'Укажите время начало среза не позднее времени окончания'
    assert resolution in ('n', 'h', 'd'), \
        "Укажите верную временную резолюцию из 'n', 'h', 'd'"
    assert '.' not in file_name, \
        'Укажите имя файла без расширения и точек'
    assert len(ti_alias) == len(ti_name), 'Укажите верный перечень ТИ и их наименований'
    # открытие книги эксель с вложенным макросом
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    workbook = excel.Workbooks.Open(os.getcwd() + r'./connection/load_data.xlsm')
    excel.Visible = False

    worksheet = workbook.ActiveSheet
    # заполнение исзодных данных для работы макроса
    worksheet.Range('B2').Value = str(begin_time).replace('-', '.')
    worksheet.Range('B3').Value = str(end_time).replace('-', '.')
    worksheet.Range('B4').Value = resolution
    worksheet.Range('B5').Value = f'{file_name}.xlsx'

    for i, (value_1, value_2) in enumerate(list(zip(ti_name, ti_alias))):
        worksheet.Range(f'D{2+i}').Value = value_1
        worksheet.Range(f'E{2+i}').Value = value_2
    # вызов макроса
    try:
        excel.Application.Run('Лист1.main')
        saved_file_name = file_name.split("\\")[-1]
        print(f'Данные получены. Сформирован файл результатов {saved_file_name}.xlsx')
    except MyCastomException as error:
        print(f'Ошибка {str(error)} при выполнении макроса')
        return None
    # закрываем книгу с макросом
    workbook.Close(SaveChanges=False)
    excel.Quit()
    # читаем данные из созданного xlsx-файла
    data = pd.read_excel(f'{file_name}.xlsx')
    data['datetime'] = pd.to_datetime(data['datetime'], format='%d.%m.%Y %H:%M:%S')
    data.rename(columns={'average_hourly_consumption': 'power_true',
                         'average_hourly_temperature': 'temperature',},
                inplace=True)
    # улаляем xlsx-файл
    if os.path.exists(f'{file_name}.xlsx'):
        os.remove(f'{file_name}.xlsx')

    return data


if __name__ == '__main__':
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        print(load_data_from_oik(begin_time=datetime.strptime('2023-06-01 01:00:00',
                                                              '%Y-%m-%d %H:%M:%S'),
                                 end_time=datetime.strptime('2023-07-01 00:00:00',
                                                            '%Y-%m-%d %H:%M:%S'),
                                 file_name=temp_dir+r'\data_from_oik'))
