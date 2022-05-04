import os
from typing import List, Union
import pandas as pd
from pandas import DataFrame
import datetime as dt


FILES_FOLDER_PATH = 'files_cambridgeshire_bedfordshire'
CREATED_FILE_NAME = 'wholesome'
COLUMNS_TO_EXTRACT = [
    'sample.samplingPoint.label',
    'sample.sampleDateTime',
    'determinand.label',
    'result',
    'determinand.unit.label',
    'sample.sampledMaterialType.label'
]
COLUMNS_TO_CREATE = [
    'sampling_point',
    'datetime',
    'temp_water',
    'ammonia',
    'nitrate',
    'lead',
    'iron',
    'pH',
    'turbidity',
    'oxygen_diss',
    'conductivity'
    'chromium',
]
determinands = {
    'Ammonia(N)': 'ammonia',
    'Temp Water': 'temp_water',
    'Nitrate-N': 'nitrate',
    'Lead - as Pb': 'lead',
    'Iron - as Fe': 'iron',
    'pH': 'pH',
    'TurbidityNTU': 'turbidity',
    'Turbidity': 'turbidity',
    'Oxygen Diss': 'oxygen_diss',
    'O Dissolved': 'oxygen_diss',
    'Cond @ 25C': 'conductivity',
    'Cond @ 20C': 'conductivity',
    'Chromium -Cr': 'chromium'
}
LOG_FILE_STR = '{} {} {}'


def read_dataframe(file_path: str, cols: Union[List['str'], None] = None) -> DataFrame:
    return pd.read_csv(file_path, usecols=cols)


def get_file_paths() -> List['str']:
    return list(map(lambda x: f'{FILES_FOLDER_PATH}/{x}', os.listdir(FILES_FOLDER_PATH)))


def prepare_data() -> None:
    unified = pd.DataFrame(columns=COLUMNS_TO_CREATE)
    water_types = [
        'RIVER / RUNNING SURFACE WATER',
        'POND / LAKE / RESERVOIR WATER'
    ]
    for path in get_file_paths():
        print(LOG_FILE_STR.format('STARTED', dt.datetime.now().isoformat(), path))
        df = read_dataframe(path, cols=COLUMNS_TO_EXTRACT)
        df = df[df['sample.sampledMaterialType.label'].isin(water_types)]
        gk = df.groupby('sample.samplingPoint.label')
        print(f'Number of sample sites: {gk.ngroups}')
        for sample_point, sample_point_group in gk:
            by_dates = sample_point_group.groupby('sample.sampleDateTime')
            for datetime, group in by_dates:
                new_row = {col: None for col in COLUMNS_TO_CREATE}
                new_row['sampling_point'] = sample_point
                new_row['datetime'] = datetime
                for index, row in group.iterrows():
                    if row['determinand.label'] in determinands:
                        new_row[determinands[row['determinand.label']]
                                ] = row['result']
                unified = unified.append(new_row, ignore_index=True)
                print(f'\rRows added: {len(unified.index)}', end='')
            print()
            print(f'Finished {sample_point}')
        print()
        print(LOG_FILE_STR.format('FINISHED', dt.datetime.now().isoformat(), path))
        print('-' * 100)
    unified.to_csv(f'{FILES_FOLDER_PATH}/{CREATED_FILE_NAME}.csv', index=False)


if __name__ == '__main__':
    prepare_data()
