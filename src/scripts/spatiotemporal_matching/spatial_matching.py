"""
Match temporally co-occurent HARPS regions to CMEs
"""
from src.cmesrc.config import SWAN_DATA_DIR, TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, ALL_MATCHING_HARPS_DATABASE, ALL_MATCHING_HARPS_DATABASE_PICKLE, MAIN_DATABASE, MAIN_DATABASE_PICKLE
from src.cmesrc.utils import get_closest_harps_timestamp, cache_swan_data
from src.cmes.cmes import CME
from src.harps.harps import Harps
from bisect import bisect_left
import numpy as np
from tqdm import tqdm
import pandas as pd
import astropy.units as u

MAX_TIME_DIFF = 0 * u.min

rows = []

def findSpatialCoOcurrentHarps():
    TEMPORAL_MATCHING_HARPS_DF = pd.read_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

    SWAN_DATA = cache_swan_data()

    final_database_rows = []

    print("\n==Finding Spatially Matching Harps.==\n")
    for idx, cme_data in tqdm(TEMPORAL_MATCHING_HARPS_DF.iterrows(), total=TEMPORAL_MATCHING_HARPS_DF.shape[0]):
        CME_DETECTION_DATE = cme_data["CME_DATE"]
        CME_PA = cme_data["CME_PA"]
        CME_WIDTH = cme_data["CME_WIDTH"]
        CME_IS_HALO = bool(cme_data["CME_HALO"])
        CME_SEEN_ONLY_IN = int(cme_data["CME_SEEN_IN"])
        HARPNUM = cme_data["HARPNUM"]

        cme = CME(CME_DETECTION_DATE, CME_PA, CME_WIDTH, halo=CME_IS_HALO, seen_only_in = CME_SEEN_ONLY_IN)

        harps_data = SWAN_DATA[HARPNUM]

        harps_timestamps = harps_data["Timestamp"].to_numpy()

        CME_CLOSEST_HARPS_TIME = get_closest_harps_timestamp(harps_timestamps, cme.DATE)

        LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = harps_data.loc[CME_CLOSEST_HARPS_TIME, ["LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]].to_numpy()

        harps = Harps(
                date = CME_CLOSEST_HARPS_TIME,
                lon_min = LON_MIN,
                lat_min = LAT_MIN,
                lon_max = LON_MAX,
                lat_max = LAT_MAX,
                HARPNUM = HARPNUM
                      )

        is_harps_within_boundary, is_harps_rotated, harps_rotated_by, out_harps = cme.hasHarpsSpatialCoOcurrence(harps, max_time_diff=0 * u.min) # Max diff 0 because I want the positions at the same time

        if out_harps.DATE != cme.DATE:
            raise ValueError("Harps time and CME time don't match")

        new_row = cme_data.to_dict()
        new_row["HARPS_SPAT_CONSIST"] = is_harps_within_boundary
        new_row["HARPS_DATE"] = out_harps.DATE.to_string()
        new_row["HARPS_MIDPOINT"] = out_harps.get_centre_point().get_raw_coords()
        new_row["HARPS_DISTANCE_TO_SUN_CENTRE"] = out_harps.get_distance_to_sun_centre()
        new_row["HARPS_PA"] = out_harps.get_position_angle()
        new_row["CME_HARPS_PA_DIFF"] = cme.get_bbox_pa_diff(out_harps)
        new_row["HARPS_RAW_BBOX"] = out_harps.get_raw_bbox()
        new_row["HARPS_LON_MIN"] = out_harps.get_raw_bbox()[0][0]
        new_row["HARPS_LAT_MIN"] = out_harps.get_raw_bbox()[0][1]
        new_row["HARPS_LON_MAX"] = out_harps.get_raw_bbox()[1][0]
        new_row["HARPS_LAT_MAX"] = out_harps.get_raw_bbox()[1][1]
#        new_row.at["harps_rotated"] = int(is_harps_rotated)
#        new_row.at["harps_rotated_by"] = harps_rotated_by
#        new_row.at["cme_time_at_sun_centre"] = cme.calculateApproximateLinearTimeAtSunCentre()

        final_database_rows.append(new_row)

    final_database = pd.DataFrame.from_records(final_database_rows)

    final_database.to_csv(ALL_MATCHING_HARPS_DATABASE, index=False)
    final_database.to_pickle(ALL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, index=False)
    final_database.to_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(MAIN_DATABASE, index=False)
    final_database.to_pickle(MAIN_DATABASE_PICKLE)


if __name__ == "__main__":
    findSpatialCoOcurrentHarps()
