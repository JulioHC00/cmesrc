from src.cmesrc.utils import cache_swan_data, clear_screen
from src.cmesrc.config import UPDATED_SWAN
from tqdm import tqdm
from src.harps.harps import Harps
import pandas as pd
import numpy as np
from os.path import join

clear_screen()

SWAN = cache_swan_data()

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
    swan_harp.to_csv(join(UPDATED_SWAN, "original.csv"), sep="\t", index=False)
    swan_harp["IRBB"] = False

    nan_bbox_mask = swan_harp[["LON_MIN", "LON_MAX", "LAT_MIN", "LAT_MAX"]].isna().any(axis=1)

    intervals = list(get_nan_intervals(nan_bbox_mask))

    for interval in intervals:
        start, end = interval

        middle = (start + end) // 2

        if end == len(swan_harp):
            end = len(swan_harp) - 1

        if start == 0:
            ref_index = end + 1
        else:
            ref_index = start - 1

        ref_row = swan_harp.iloc[ref_index]

        harp_data = ref_row[["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()

        harps = Harps(*harp_data)

        for i in range(start, end):
            if i <= middle and start != 0:
                ref_index = start - 1
            elif start == 0:
                ref_index = end
            
            elif i > middle and end != len(swan_harp):
                ref_index = end

            elif end == len(swan_harp):
                ref_index = start - 1
            
            else:
                raise ValueError("Something went wrong")

            ref_row = swan_harp.iloc[ref_index]

            harp_data = ref_row[["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()

            harps = Harps(*harp_data)

            incomplete_row = swan_harp.iloc[i]

            new_timestamp = incomplete_row["Timestamp"]

            rotated_harps = harps.rotate_bbox(new_timestamp)

            new_bbox = rotated_harps.get_raw_bbox()

            swan_harp.at[i, "LON_MIN"] = new_bbox[0][0]
            swan_harp.at[i, "LAT_MIN"] = new_bbox[0][1]
            swan_harp.at[i, "LON_MAX"] = new_bbox[1][0]
            swan_harp.at[i, "LAT_MAX"] = new_bbox[1][1]

            swan_harp.at[i, "IRBB"] = True

    filename = f"{harpnum}.csv" 

    swan_harp.to_csv(join(UPDATED_SWAN, filename), sep="\t", index=False)