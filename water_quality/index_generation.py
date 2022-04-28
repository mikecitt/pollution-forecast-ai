import os
from typing import List, Union
import pandas as pd
import pandasql as ps
from pandas import DataFrame
import datetime as dt


FILES_FOLDER_PATH = 'files_cambridgeshire_bedfordshire'
FILE_NAME = 'wholesome.csv'
NEW_FILE_NAME = 'cambridgeshire_bedfordshire.csv'

who_standard = {
    'temp_water': 25, # (°C)
    'ammonia': 0.2, # (mg/l)
    'nitrate': 50, # (mg/l)
    'lead': 10, # (ug/l)
    'iron': 300, # (ug/l)
    'pH': 8.5, # (pH units)
    'turbidity': 5, # (FTU)
    'oxygen_diss': 5, # (mg/l)
    'conductivity': 300, # (us/cm)
    'chromium': 50 # (ug/l)
}

ideal_values = {
    'temp_water': 20, # (°C)
    'ammonia': 0, # (mg/l)
    'nitrate': 0, # (mg/l)
    'lead': 0, # (ug/l)
    'iron': 0, # (ug/l)
    'pH': 7, # (pH units)
    'turbidity': 0, # (FTU)
    'oxygen_diss': 0, # (mg/l)
    'conductivity': 0, # (us/cm)
    'chromium': 0 # (ug/l)
}


def group_data() -> DataFrame:
    df = pd.read_csv(os.path.join(FILES_FOLDER_PATH, FILE_NAME))
    query = """
        SELECT 
                SUBSTR(datetime, 1, 10) AS date, 
                ROUND(AVG(temp_water), 2) AS temp_water,
                ROUND(AVG(ammonia), 2) AS ammonia,
                ROUND(AVG(nitrate), 2) AS nitrate,
                ROUND(AVG(lead), 2) AS lead,
                ROUND(AVG(iron), 2) AS iron,
                ROUND(AVG(pH), 2) AS pH,
                ROUND(AVG(turbidity), 2) AS turbidity,
                ROUND(AVG(oxygen_diss), 2) AS oxygen_diss,
                ROUND(AVG(conductivity), 2) AS conductivity,
                ROUND(AVG(chromium), 2) AS chromium
        FROM df 
        GROUP BY date
        ORDER BY date ASC
    """

    return ps.sqldf(query, locals())


def fill_empty_values(df: DataFrame) -> DataFrame:
    columns = list(who_standard.keys())
    df[columns] = df[columns].fillna(df[columns].median().round(2))
    return df
    

def calculate_index(df: DataFrame) -> None:
    K = 1 / sum(map(lambda x: 1.0 / x, list(who_standard.values())))
    Wi = {k: K / v  for k, v in who_standard.items() }
    wqi = None
    for key, value in ideal_values.items():
        qi = Wi[key] * ((df[key] - value) / (who_standard[key] - value)) * 100 
        if wqi is None:
            wqi = qi
        else:
            wqi += qi

    df['wqi'] = wqi
    df = df.round({'wqi': 2})
    df.to_csv(os.path.join(FILES_FOLDER_PATH, NEW_FILE_NAME), index=False)


if __name__ == '__main__':
    df = group_data()
    df = fill_empty_values(df)
    calculate_index(df)