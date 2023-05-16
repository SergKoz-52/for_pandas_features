import pandas as pd
from json import dumps, loads

"""
Функция add_json_col добавляет или обновляет поле Pandas DataFrame с 
json значениями
Examples:
    df = pd.DataFrame(
    {'A': [1,2,3], 'B': ['w', 'e', 'p'], 'C': ['2023', '2022', '2021']})
    df
    
        A  B     C
        0  1  w  2023
        1  2  e  2022
        2  3  p  2021

    df1 = add_json_col(df, 'a_key', 'A')
    df1
    
    A  B     C     description
    0  1  w  2023  {"a_key": "1"}
    1  2  e  2022  {"a_key": "2"}
    2  3  p  2021  {"a_key": "3"}

    df2 = add_json_col(df, 'example', 'abc', col_value=False)
    df2
    
    A  B     C         description
    0  1  w  2023  {"example": "abc"}
    1  2  e  2022  {"example": "abc"}
    2  3  p  2021  {"example": "abc"}
    
    df3 = add_json_col(df2, 'B', 'B')
    df3
    
    A  B     C                   description
    0  1  w  2023  {"example": "abc", "B": "w"}
    1  2  e  2022  {"example": "abc", "B": "e"}
    2  3  p  2021  {"example": "abc", "B": "p"}
    
    df4 = add_json_col(
        df, 'example', 'test', col_value=False, accum_col='json_val')
    df4
    
    A  B     C             json_val
    0  1  w  2023  {"example": "test"}
    1  2  e  2022  {"example": "test"}
    2  3  p  2021  {"example": "test"}
"""


def add_json_col(
        df_src: pd.DataFrame,
        key: str,
        value: str,
        col_value: bool = True,
        accum_col: str = 'description') -> pd.DataFrame:

    """
    :param df_src: Pandas DataFrame. DataFrame Который необходимо изменить
    :param key: str. Ключа(поле) аналогично key dict
    :param value: str. Значение аналогично key dict
    :param col_value: bool, default True. Значение будет использовано
    из указанного поля DataFrame, если такое существует или выдаст ошибку
    :param accum_col: str, default 'description'. Наименование  поля(колонки)
    которое надо создать или обновить
    :return: Измененный Pandas DataFrame
    """

    df = df_src.copy()

    def process_map_col(_df: pd.DataFrame) -> pd.DataFrame:
        for row, col in _df.iterrows():
            val_to_dict = loads(_df.loc[row, accum_col])
            if col_value:
                val = str(_df.loc[row, value])
            else:
                val = value
            val_to_dict.update({key: val})
            val_to_dict = dumps(val_to_dict)
            _df.loc[row, accum_col] = val_to_dict
        return _df

    if col_value:
        if value in df.columns:
            if accum_col not in df.columns:
                df[accum_col] = '{' + f'"{key}": ' + '"' + df[value].astype(
                    str) + '"' + '}'
            else:
                try:
                    df = process_map_col(df)
                except Exception as err:
                    msg = 'Maybe field values are nit in Json ' \
                          'format "{"key": "val"}"'
                    print(err)
                    raise ValueError(msg)
        else:
            raise ValueError(f"column {value} not in df.columns!")

    else:
        if accum_col not in df.columns:
            df[accum_col] = '{' + f'"{key}": ' + '"' + value + '"' + '}'
        else:
            try:
                df = process_map_col(df)
            except Exception as err:
                msg = 'Maybe field values are nit in Json ' \
                      'format "{"key": "val"}"'
                print(err)
                raise ValueError(msg)

    return df
