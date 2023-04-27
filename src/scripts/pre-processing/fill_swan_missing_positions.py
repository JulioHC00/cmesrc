from src.cmesrc.utils import cache_swan_data, clear_screen
from src.cmesrc.config import UPDATED_SWAN
from tqdm import tqdm
from src.harps.harps import Harps
import pandas as pd
import numexpr as ne
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
    nan_bbox_mask = swan_harp[["LON_MIN", "LON_MAX", "LAT_MIN", "LAT_MAX"]].isna().any(axis=1)

    new_swan_harp = swan_harp.copy()
    new_swan_harp["IRBB"] = False

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

        first_harp_data = swan_harp.iloc[first_index][["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()
        last_harp_data = swan_harp.iloc[last_index][["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()

        first_harps = Harps(*first_harp_data)
        last_harps = Harps(*last_harp_data)

        harps = [first_harps if i <= middle else last_harps for i in range(start, end)]

        incomplete_rows = swan_harp.iloc[start:end]
        incomplete_indices = incomplete_rows.index

        new_timestamps = incomplete_rows["Timestamp"].to_numpy()
        new_bboxes = np.array([harps[i].rotate_bbox(new_timestamps[i]).get_raw_bbox() for i in range(len(harps))])

        new_swan_harp.loc[incomplete_indices, "LON_MIN"] = new_bboxes[:, 0, 0]
        new_swan_harp.loc[incomplete_indices, "LAT_MIN"] = new_bboxes[:, 0, 1]
        new_swan_harp.loc[incomplete_indices, "LON_MAX"] = new_bboxes[:, 1, 0]
        new_swan_harp.loc[incomplete_indices, "LAT_MAX"] = new_bboxes[:, 1, 1]

        new_swan_harp.loc[incomplete_indices, "IRBB"] = True

    filename = f"{harpnum}.csv" 

    new_swan_harp.to_csv(join(UPDATED_SWAN, filename), sep="\t", index=False)