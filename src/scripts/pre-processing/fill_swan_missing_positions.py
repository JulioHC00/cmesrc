from src.cmesrc.utils import cache_swan_data, clear_screen
from src.cmesrc.config import UPDATED_SWAN
from tqdm import tqdm
from src.harps.harps import Harps
import pandas as pd
import numpy as np
from os.path import join

clear_screen()

SWAN = cache_swan_data()

# SWAN = {
#     "test": pd.read_csv("/home/julio/cmesrc/data/raw/mvts/SWAN/partition4/4588.csv", sep="\t")
# }

clear_screen()

print("== FILLING MISSING VALUES ==")

def get_nan_intervals(nan_bbox_mask):
    start = None
    end = None

    for i, value in enumerate(nan_bbox_mask):
        if value and start is None:
            start = i

        elif not value and start is not None:
            end = i

            yield start, end

            start = None
            end = None

        if i == len(nan_bbox_mask) - 1 and start is not None:
            end = i + 1

            yield start, end

            start = None
            end = None
        

for harpnum, swan_harp in tqdm(SWAN.items()):
    new_swan_harp = swan_harp.copy()

    new_swan_harp["IRBB"] = False

    nan_bbox_mask = swan_harp[["LON_MIN", "LON_MAX", "LAT_MIN", "LAT_MAX"]].isna().any(axis=1)

    intervals = list(get_nan_intervals(nan_bbox_mask))

    for interval in intervals:
        start, end = interval

        middle = (start + end) // 2

        if start != 0:
            first_index = start - 1
        else:
            first_index = end

        if end != len(new_swan_harp):
            last_index = end
        else:
            last_index = start - 1

        first_ref_row = swan_harp.iloc[first_index]
        last_ref_row = swan_harp.iloc[last_index]

        first_harp_data = first_ref_row[["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()
        last_harp_data = last_ref_row[["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()

        try:
            first_harps = Harps(*first_harp_data)
        except (ValueError, TypeError):
            print("FIRST HARP DATA")
            print(harpnum)
            print(first_harp_data)
            raise

        try:
            last_harps = Harps(*last_harp_data)
        except (ValueError, TypeError):
            print("LAST HARP DATA")
            print(harpnum)
            print(last_harp_data)
            print(last_ref_row)
            raise

        for i in range(start, end):
            if i <= middle:
                harps = first_harps
            else:
                harps = last_harps

            incomplete_row = swan_harp.iloc[i]
            incomplete_index = incomplete_row.name

            new_timestamp = incomplete_row["Timestamp"]

            rotated_harps = harps.rotate_bbox(new_timestamp)

            new_bbox = rotated_harps.get_raw_bbox()

            new_swan_harp.at[incomplete_index, "LON_MIN"] = new_bbox[0][0]
            new_swan_harp.at[incomplete_index, "LAT_MIN"] = new_bbox[0][1]
            new_swan_harp.at[incomplete_index, "LON_MAX"] = new_bbox[1][0]
            new_swan_harp.at[incomplete_index, "LAT_MAX"] = new_bbox[1][1]

            new_swan_harp.at[incomplete_index, "IRBB"] = True

    filename = f"{harpnum}.csv" 

    new_swan_harp.to_csv(join(UPDATED_SWAN, filename), sep="\t", index=False)