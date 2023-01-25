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
cme_detection_times = np.array([Time(cme_time) for cme_time in lasco_cme_database["date"]])

FIRST_AVAILABLE_HARPS = min(harps_lifetime_start_times)
 
CME_TIME_MASK = cme_detection_times > FIRST_AVAILABLE_HARPS
CME_QUALITY_MASK = (lasco_cme_database["quality"] == 0) & (lasco_cme_database["three_points"] == 0) # CMEs withou poor or very poor flags and without few points warning.

def findMatchingHarpsRegions(cme_detection_time):

    matching_harps_mask = (harps_lifetime_start_times <= cme_detection_time) & (harps_lifetime_end_times >= cme_detection_time)

    matching_harps_list = harps_lifetime_database[matching_harps_mask]["harpsnum"].to_numpy()

    cme_date_with_harps_list = {
            "cme_date": str(cme_detection_time),
            "matching_harps": matching_harps_list,
            "n_matching_harps": len(matching_harps_list)
            }

    return cme_date_with_harps_list

def findAllMatchingRegions():
    print("Finding HARPS that match CMEs temporally")
    full_list_of_matches = []
#    full_list_of_matches = [findMatchingHarpsRegions(cme_detection_time) for cme_detection_time in cme_detection_times[CME_MASK]]
    for cme_detection_time in tqdm(cme_detection_times[CME_TIME_MASK & CME_QUALITY_MASK]):
        full_list_of_matches.append(findMatchingHarpsRegions(cme_detection_time) )

    temporal_matching_harps_database = pd.DataFrame.from_records(full_list_of_matches)
    temporal_matching_harps_database.to_csv(TEMPORAL_MATCHING_HARPS_DATABASE, index=False)

if __name__ == "__main__":
    findAllMatchingRegions()
