from astropy.time import Time
from datetime import datetime, timedelta
import astropy.units as u
from bisect import bisect_left
import sqlite3
from tqdm import tqdm
import numpy as np
from os import walk, system, name
from os.path import join
import pandas as pd
from src.cmesrc.config import DT_SWAN_DATA_DIR, SWAN_DATA_DIR, UPDATED_SWAN

def clear_screen(): # for windows
    if name == 'nt':
        _ = system('cls')
 
    # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')

def parse_date(date_str):
    if type(date_str) == Time:
        return date_str
    elif type(date_str) == str:
        return Time(date_str)
    else:
        raise ValueError("Input date must be either a string or a astropy Time object")

def parse_pandas_str_list(str_list: str) -> list:
    no_brackets_str_list = str_list.replace("[", "").replace("]", "").split()
    parsed_list = [int(item) for item in no_brackets_str_list]
    return parsed_list

def get_closest_harps_timestamp(harps_timestamps, cme_time) -> Time:
    i = bisect_left(harps_timestamps, cme_time)
    return min(harps_timestamps[max(0, i-1): i+2], key=lambda t: abs(cme_time - t))

def cache_swan_data() -> dict:
    clear_screen()
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t")

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    clear_screen()
    return data_dict

def cache_dt_swan_data() -> dict:
    clear_screen()
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(DT_SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t")

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    clear_screen()
    return data_dict

def filepaths_dt_swan_data() -> dict:
    clear_screen()
    print("\n==CACHING SWAN DATA.==\n")
    data_paths = dict()

    for directoryName, subdirectoryName, fileList in walk(DT_SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            filepath = join(directoryName, fileName)

            data_paths[harpnum] = filepath

    clear_screen()
    return data_paths

def filepaths_updated_swan_data() -> dict:
    clear_screen()
    print("\n==CACHING SWAN DATA.==\n")
    data_paths = dict()

    for directoryName, subdirectoryName, fileList in walk(UPDATED_SWAN):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            filepath = join(directoryName, fileName)

            data_paths[harpnum] = filepath

    clear_screen()
    return data_paths

def read_SWAN_filepath(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep="\t")

    timestamps = list(df["Timestamp"].to_numpy())

    df['Timestamp'] = Time(timestamps, format="iso")

    df.set_index("Timestamp", drop=False, inplace=True)

    return df

def cache_updated_swan_data() -> dict:
    clear_screen()
    print("\n==CACHING UPDATED SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(UPDATED_SWAN):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t")

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    clear_screen()
    return data_dict

def read_sql_processed_bbox(harpnum: int, conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(
        f"""
        SELECT * FROM PROCESSED_HARPS_BBOX
        WHERE harpnum = {harpnum}
        """
        , conn
    )

    timestamps = list(df["timestamp"].to_numpy())

    df['timestamp'] = Time(timestamps, format="iso")
    df['Timestamp'] = df['timestamp']

    df.set_index("timestamp", drop=False, inplace=True)

    return df