"""
Will match CMEs with HARPS regions that were present on-disk at the time of the CME.
HERE THE LASCO CME DATABASE IS MASKED TO ONLY CMES WITHOUT POOR OR VERY POOR DESCRIPTIONS AND NO N POINTS WARNINGS
"""

from src.cmesrc.config import (
    LASCO_CME_DATABASE,
    TEMPORAL_MATCHING_HARPS_DATABASE,
    TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE,
    CMESRC_BBOXES,
)
from src.cmesrc.utils import clear_screen
import numpy as np
from tqdm import tqdm
from astropy.time import Time
from bisect import bisect_right
import pandas as pd
import sqlite3

conn = sqlite3.connect(CMESRC_BBOXES)
cur = conn.cursor()

lasco_cme_database = pd.read_csv(LASCO_CME_DATABASE)

# There are 2 pairs of CMEs with exact same date and PA with different widths. I honestly don't know how that's possible
# I'm just going to remove them

lasco_cme_database = lasco_cme_database[
    ~lasco_cme_database.duplicated(subset=["CME_ID"], keep=False)
]

harps_lifetime_database = pd.read_sql(
    """
                                      SELECT * FROM HARPS
                                      WHERE harpnum IN (SELECT DISTINCT harpnum FROM PROCESSED_HARPS_BBOX)
                                      """,
    conn,
)

harps_lifetime_start_times = np.array(
    [Time(harps_time) for harps_time in harps_lifetime_database["start"]]
)
harps_lifetime_end_times = np.array(
    [Time(harps_time) for harps_time in harps_lifetime_database["end"]]
)
lasco_cme_database["CME_DATE"] = np.array(
    [Time(cme_time) for cme_time in lasco_cme_database["CME_DATE"]]
)  # Parse dates
cme_detection_times = lasco_cme_database["CME_DATE"].to_numpy()

harpsnums = harps_lifetime_database["harpnum"].to_numpy()

FIRST_AVAILABLE_HARPS = min(harps_lifetime_start_times)
LAST_AVAILABLE_HARPS = max(harps_lifetime_end_times)

CME_TIME_MASK = np.array(cme_detection_times >= FIRST_AVAILABLE_HARPS) & np.array(
    (cme_detection_times <= LAST_AVAILABLE_HARPS)
)
CME_QUALITY_MASK = lasco_cme_database["CME_QUALITY"] == 0
CME_FULL_MASK = CME_TIME_MASK & CME_QUALITY_MASK

masked_lasco_cme_database = lasco_cme_database[CME_FULL_MASK]
masked_cme_times = np.array(
    [Time(cme_time) for cme_time in masked_lasco_cme_database["CME_DATE"]]
)


def findAllMatchingRegions():
    print("== Finding HARPS that match CMEs temporally ==")

    sorted_start_indices = np.argsort(harps_lifetime_start_times)
    sorted_end_indices = np.argsort(harps_lifetime_end_times)

    harps_lifetime_start_times_sorted = harps_lifetime_start_times[sorted_start_indices]
    harps_lifetime_end_times_sorted = harps_lifetime_end_times[sorted_end_indices]

    full_list_of_matches = []
    new_data_rows = []

    n = len(harpsnums)

    for cme_time in tqdm(masked_cme_times):
        start_index = bisect_right(harps_lifetime_start_times_sorted, cme_time)
        end_index = bisect_right(harps_lifetime_end_times_sorted, cme_time)

        start_indices = sorted_start_indices[:start_index]
        end_indices = sorted_end_indices[end_index:]

        harps = harpsnums[np.intersect1d(start_indices, end_indices)]

        full_list_of_matches.append(list(harps))

    for i, harpnum_list in enumerate(full_list_of_matches):
        for harpnum in harpnum_list:
            new_row = masked_lasco_cme_database.iloc[i].to_dict()
            new_row["HARPNUM"] = harpnum
            new_row["CME_HARPNUM_ID"] = f"{new_row['CME_ID']}{harpnum}"
            new_data_rows.append(new_row)

    temporal_matching_harps_database = pd.DataFrame.from_records(new_data_rows)
    columns = temporal_matching_harps_database.columns
    first_cols = ["CME_ID", "CME_HARPNUM_ID", "CME_DATE", "HARPNUM"]
    temporal_matching_harps_database = temporal_matching_harps_database[
        first_cols + [col for col in columns if col not in first_cols]
    ]

    # NOT ANYMORE, SOLVED
    # Some CMEs have the exact same time but are supposedly different ones, this makes life hard because if they share a HARPS region then I will get duplicated IDs. I'll just remove duplicated.

    # temporal_matching_harps_database.drop_duplicates(subset=["CME_HARPNUM_ID"], inplace=True)
    temporal_matching_harps_database.to_csv(
        TEMPORAL_MATCHING_HARPS_DATABASE, index=False
    )
    temporal_matching_harps_database.to_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)


if __name__ == "__main__":
    clear_screen()

    findAllMatchingRegions()

    clear_screen()
