from astropy.time import Time
from datetime import datetime, timedelta
import astropy.units as u
from bisect import bisect_left
from tqdm import tqdm
from os import walk
from os.path import join
import pandas as pd
from src.cmesrc.config import SWAN_DATA_DIR

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
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t", na_values="None", usecols=['Timestamp', 'LAT_MIN', 'LON_MIN', 'LAT_MAX', 'LON_MAX']).dropna()

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    return data_dict
