"""
Will match CMEs with HARPS regions that were present on-disk at the time of the CME.
HERE THE LASCO CME DATABASE IS MASKED TO ONLY CMES WITHOUT POOR OR VERY POOR DESCRIPTIONS AND NO N POINTS WARNINGS
"""

from src.cmesrc.config import LASCO_CME_DATABASE, HARPS_LIFETIME_DATABSE, TEMPORAL_MATCHING_HARPS_DATABASE
import numpy as np
from tqdm import tqdm
from astropy.time import Time
import pandas as pd

lasco_cme_database = pd.read_csv(LASCO_CME_DATABASE)
harps_lifetime_database = pd.read_csv(HARPS_LIFETIME_DATABSE)

harps_lifetime_start_times = np.array([Time(harps_time) for harps_time in harps_lifetime_database["start"]])
harps_lifetime_end_times = np.array([Time(harps_time) for harps_time in harps_lifetime_database["end"]])
lasco_cme_database["date"] = np.array([Time(cme_time) for cme_time in lasco_cme_database["date"]]) # Parse dates
cme_detection_times = lasco_cme_database["date"].to_numpy()

FIRST_AVAILABLE_HARPS = min(harps_lifetime_start_times)
 
CME_TIME_MASK = cme_detection_times > FIRST_AVAILABLE_HARPS
CME_QUALITY_MASK = (lasco_cme_database["quality"] == 0) & (lasco_cme_database["three_points"] == 0) # CMEs withou poor or very poor flags and without few points warning.
CME_FULL_MASK = CME_TIME_MASK & CME_QUALITY_MASK

masked_lasco_cme_database = lasco_cme_database[CME_FULL_MASK]

def findMatchingHarpsRegions(cme_detection_time: str) -> np.ndarray:

    matching_harps_mask = (harps_lifetime_start_times <= cme_detection_time) & (harps_lifetime_end_times >= cme_detection_time)

    matching_harps_list = harps_lifetime_database[matching_harps_mask]["harpsnum"].to_numpy()

    return matching_harps_list


def findAllMatchingRegions():
    print("Finding HARPS that match CMEs temporally")
    full_list_of_matches = []

    for i, row in tqdm(masked_lasco_cme_database.iterrows(), total=masked_lasco_cme_database.shape[0]):
        cme_detection_time = row["date"]
        matching_harps_list = findMatchingHarpsRegions(cme_detection_time)

        new_row = row.to_dict()

        new_row["matching_harps"] = matching_harps_list
        new_row["n_matching_harps"] = len(matching_harps_list)

        full_list_of_matches.append(new_row)

    temporal_matching_harps_database = pd.DataFrame.from_records(full_list_of_matches)
    temporal_matching_harps_database.to_csv(TEMPORAL_MATCHING_HARPS_DATABASE, index=False)

if __name__ == "__main__":
    findAllMatchingRegions()
