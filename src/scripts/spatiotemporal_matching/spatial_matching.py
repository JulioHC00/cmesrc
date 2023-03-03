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

EXTRA_CME_WIDTH = 10
HALO_MAX_SUN_CENTRE_DIST = 0.5

rows = []

def findSpatialCoOcurrentHarps():
    temporal_matching_harps = pd.read_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    temporal_matching_harps.set_index("CME_HARPNUM_ID", inplace=True, drop=False)
    temporal_matching_harps = temporal_matching_harps.sort_values(by="HARPNUM")
    grouped_matching_harps = temporal_matching_harps.groupby("HARPNUM") 
    final_database = temporal_matching_harps.copy()

    SWAN_DATA = cache_swan_data()

    final_database_rows = []
    harps_indices = None
    ALL_LON_MIN = []
    ALL_LAT_MIN = []
    ALL_LON_MAX = []
    ALL_LAT_MAX = []
    ALL_RAW_HARPS_TIMES = []

    print("\n===Finding Spatially Matching Harps.===\n")
    print("\n=Finding Closest Harps Positions=\n")
    for harpnum, group in tqdm(grouped_matching_harps):
        harps_data = SWAN_DATA[harpnum]
        cme_times = group["CME_DATE"].to_numpy()
        harps_timestamps = harps_data["Timestamp"].to_numpy()
        if harps_indices is None:
            harps_indices = group.index
        else:
            harps_indices = harps_indices.append(group.index)

        cme_closest_harps_time_indices = []
        for cme_time in cme_times:
            cme_closest_harps_time_indices.append(get_closest_harps_timestamp(harps_timestamps, cme_time))

        LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = harps_data.loc[cme_closest_harps_time_indices, ["LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]].to_numpy().T

        ALL_LON_MIN.extend(list(LON_MIN))
        ALL_LAT_MIN.extend(list(LAT_MIN))
        ALL_LON_MAX.extend(list(LON_MAX))
        ALL_LAT_MAX.extend(list(LAT_MAX))
        ALL_RAW_HARPS_TIMES.extend(list(cme_closest_harps_time_indices))

    final_database.at[harps_indices, "HARPS_RAW_LON_MIN"] = ALL_LON_MIN
    final_database.at[harps_indices, "HARPS_RAW_LAT_MIN"] = ALL_LAT_MIN
    final_database.at[harps_indices, "HARPS_RAW_LON_MAX"] = ALL_LON_MAX
    final_database.at[harps_indices, "HARPS_RAW_LAT_MAX"] = ALL_LAT_MAX
    final_database.at[harps_indices, "HARPS_RAW_DATE"] = ALL_RAW_HARPS_TIMES


    cme_grouped_matching_harps = final_database.groupby("CME_ID")

    final_database["HARPS_DATE"] = None
    final_database["HARPS_MIDPOINT"] = None
    final_database["HARPS_DISTANCE_TO_SUN_CENTRE"] = None
    final_database["HARPS_PA"] = None
    final_database["CME_HARPS_PA_DIFF"] = None
    final_database["HARPS_RAW_BBOX"] = None
    final_database["HARPS_LON_MIN"] = None
    final_database["HARPS_LAT_MIN"] = None
    final_database["HARPS_LON_MAX"] = None
    final_database["HARPS_LAT_MAX"] = None

    print("\n=Rotating Harps Positions=\n")
    for cme_id, group in tqdm(cme_grouped_matching_harps):
        CME_DETECTION_DATE = group.iloc[0]["CME_DATE"]
        CME_PA = group.iloc[0]["CME_PA"]
        CME_WIDTH = group.iloc[0]["CME_WIDTH"]
        CME_IS_HALO = bool(group.iloc[0]["CME_HALO"])
        CME_SEEN_ONLY_IN = int(group.iloc[0]["CME_SEEN_IN"])

        cme = CME(CME_DETECTION_DATE, CME_PA, CME_WIDTH, halo=CME_IS_HALO, seen_only_in = CME_SEEN_ONLY_IN)

        for idx, harps_data in group.iterrows():
            HARPS_DATE = harps_data["HARPS_RAW_DATE"]
            LON_MIN = harps_data["HARPS_RAW_LON_MIN"]
            LAT_MIN = harps_data["HARPS_RAW_LAT_MIN"]
            LON_MAX = harps_data["HARPS_RAW_LON_MAX"]
            LAT_MAX = harps_data["HARPS_RAW_LAT_MAX"]
            HARPNUM = harps_data["HARPNUM"]


            harps = Harps(
                    date = HARPS_DATE,
                    lon_min = LON_MIN,
                    lat_min = LAT_MIN,
                    lon_max = LON_MAX,
                    lat_max = LAT_MAX,
                    HARPNUM = HARPNUM
                          )

#            final_database.at[harps_data.index, "HARPS_SPAT_CONSIST"] = is_harps_within_boundary
            final_database.at[idx, "HARPS_DATE"] = harps.DATE.to_string()
            final_database.at[idx, "HARPS_MIDPOINT"] = harps.get_centre_point().get_raw_coords()
            final_database.at[idx, "HARPS_DISTANCE_TO_SUN_CENTRE"] = harps.get_distance_to_sun_centre()
            final_database.at[idx, "HARPS_PA"] = harps.get_position_angle()
            final_database.at[idx, "CME_HARPS_PA_DIFF"] = cme.get_bbox_pa_diff(harps)
            final_database.at[idx, "HARPS_RAW_BBOX"] = harps.get_raw_bbox()
            final_database.at[idx, "HARPS_LON_MIN"] = harps.get_raw_bbox()[0][0]
            final_database.at[idx, "HARPS_LAT_MIN"] = harps.get_raw_bbox()[0][1]
            final_database.at[idx, "HARPS_LON_MAX"] = harps.get_raw_bbox()[1][0]
            final_database.at[idx, "HARPS_LAT_MAX"] = harps.get_raw_bbox()[1][1]


    non_halo = final_database[final_database["CME_HALO"] == 0]
    halo = final_database[final_database["CME_HALO"] == 1]

    non_halo_matching = non_halo["CME_HARPS_PA_DIFF"] < (non_halo["CME_WIDTH"] / 2 + EXTRA_CME_WIDTH)
    halo_matching = halo["HARPS_DISTANCE_TO_SUN_CENTRE"] < HALO_MAX_SUN_CENTRE_DIST

    matches = pd.concat([non_halo_matching, halo_matching], axis=0, sort=True)

    final_database = pd.concat([final_database, matches.rename("HARPS_SPAT_CONSIST")], axis=1)

    final_database.sort_values(by=["CME_DATE", "HARPNUM"], inplace=True)

#    final_database.to_csv(ALL_MATCHING_HARPS_DATABASE, index=False)
#    final_database.to_pickle(ALL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, index=False)
    final_database.to_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(MAIN_DATABASE, index=False)
    final_database.to_pickle(MAIN_DATABASE_PICKLE)


if __name__ == "__main__":
    findSpatialCoOcurrentHarps()
