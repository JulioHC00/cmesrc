"""
Will match CMEs with HARPS regions that were present on-disk at the time of the CME.
HERE THE LASCO CME DATABASE IS MASKED TO ONLY CMES WITHOUT POOR OR VERY POOR DESCRIPTIONS AND NO N POINTS WARNINGS
"""

from src.cmesrc.config import LASCO_CME_DATABASE, HARPS_LIFETIME_DATABSE, TEMPORAL_MATCHING_HARPS_DATABASE, TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE
import numpy as np
from tqdm import tqdm
from astropy.time import Time
import pandas as pd

lasco_cme_database = pd.read_csv(LASCO_CME_DATABASE)
harps_lifetime_database = pd.read_csv(HARPS_LIFETIME_DATABSE)

harps_lifetime_start_times = np.array([Time(harps_time) for harps_time in harps_lifetime_database["start"]])
harps_lifetime_end_times = np.array([Time(harps_time) for harps_time in harps_lifetime_database["end"]])
lasco_cme_database["CME_DATE"] = np.array([Time(cme_time) for cme_time in lasco_cme_database["CME_DATE"]]) # Parse dates
cme_detection_times = lasco_cme_database["CME_DATE"].to_numpy()

FIRST_AVAILABLE_HARPS = min(harps_lifetime_start_times)
 
CME_TIME_MASK = cme_detection_times > FIRST_AVAILABLE_HARPS
CME_QUALITY_MASK = (lasco_cme_database["CME_QUALITY"] == 0) & (lasco_cme_database["CME_THREE_POINTS"] == 0) # CMEs withou poor or very poor flags and without few points warning.
CME_FULL_MASK = CME_TIME_MASK & CME_QUALITY_MASK

masked_lasco_cme_database = lasco_cme_database[CME_FULL_MASK]

def findMatchingHarpsRegions(cme_detection_time: str) -> np.ndarray:

    matching_harps_mask = (harps_lifetime_start_times <= cme_detection_time) & (harps_lifetime_end_times >= cme_detection_time)

    matching_harps_list = harps_lifetime_database[matching_harps_mask]["harpsnum"].to_numpy()

    return matching_harps_list


def findAllMatchingRegions():
    print("== Finding HARPS that match CMEs temporally ==")
    full_list_of_matches = []

    for i, row in tqdm(masked_lasco_cme_database.iterrows(), total=masked_lasco_cme_database.shape[0]):
        cme_detection_time = row["CME_DATE"]
        matching_harps_list = findMatchingHarpsRegions(cme_detection_time)


        for harpnum in matching_harps_list:
            new_row = row.to_dict()
            new_row["HARPNUM"] = harpnum
            new_row["CME_HARPNUM_ID"] = f"{new_row['CME_ID']}{harpnum}"
            full_list_of_matches.append(new_row)

    temporal_matching_harps_database = pd.DataFrame.from_records(full_list_of_matches)
    columns = temporal_matching_harps_database.columns
    first_cols = ["CME_ID", "CME_HARPNUM_ID", "CME_DATE", "HARPNUM"]
    temporal_matching_harps_database = temporal_matching_harps_database[first_cols + [col for col in columns if col not in first_cols]]

    # Some CMEs have the exact same time but are supposedly different ones, this makes life hard because if they share a HARPS region then I will get duplicated IDs. I'll just remove duplicated.

    temporal_matching_harps_database.drop_duplicates(subset=["CME_HARPNUM_ID"], inplace=True)
    temporal_matching_harps_database.to_csv(TEMPORAL_MATCHING_HARPS_DATABASE, index=False)
    temporal_matching_harps_database.to_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

if __name__ == "__main__":
    findAllMatchingRegions()
