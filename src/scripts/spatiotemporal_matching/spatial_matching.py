"""
Match temporally co-occurent HARPS regions to CMEs
"""
from src.cmesrc.config import (
    TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE,
    SPATIOTEMPORAL_MATCHING_HARPS_DATABASE,
    SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE,
    MAIN_DATABASE,
    MAIN_DATABASE_PICKLE,
    CMESRCV3_DB,
)
from src.cmesrc.utils import (
    get_closest_harps_timestamp,
    clear_screen,
    read_sql_processed_bbox,
)
from src.cmes.cmes import CME
from src.harps.harps import Harps
import numpy as np
from tqdm import tqdm
import pandas as pd
import multiprocessing as mp
import astropy.units as u
import sqlite3
from src.cmesrc.exception_classes import InvalidBoundingBox

conn = sqlite3.connect(CMESRCV3_DB)
cur = conn.cursor()

# Let's create an index in case it doesn't exist to make things faster

cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_harpnum ON PROCESSED_HARPS_BBOX (HARPNUM);"
)

EXTRA_CME_WIDTH = 10
HALO_MAX_SUN_CENTRE_DIST = 1

rows = []


def setup():
    temporal_matching_harps = pd.read_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    temporal_matching_harps.set_index("CME_HARPNUM_ID", inplace=True, drop=False)
    temporal_matching_harps = temporal_matching_harps.sort_values(by="HARPNUM")
    grouped_matching_harps = temporal_matching_harps.groupby("HARPNUM")
    final_database = temporal_matching_harps.copy()
    harps_indices = None
    ALL_LONDTMIN = []
    ALL_LATDTMIN = []
    ALL_LONDTMAX = []
    ALL_LATDTMAX = []
    ALL_RAW_HARPS_TIMES = []

    print("\n===Finding Spatially Matching Harps.===\n")
    print("\n=Finding Closest Harps Positions=\n")
    for harpnum, group in tqdm(grouped_matching_harps):
        harps_data = read_sql_processed_bbox(harpnum, conn)
        cme_times = group["CME_DATE"].to_numpy()
        harps_timestamps = harps_data["Timestamp"].to_numpy()
        if harps_indices is None:
            harps_indices = group.index
        else:
            harps_indices = harps_indices.append(group.index)

        cme_closest_harps_time_indices = []
        for cme_time in cme_times:
            closest_timestamps = get_closest_harps_timestamp(harps_timestamps, cme_time)
            cme_closest_harps_time_indices.append(closest_timestamps)

        LONDTMIN, LATDTMIN, LONDTMAX, LATDTMAX = (
            harps_data.loc[
                cme_closest_harps_time_indices,
                ["LONDTMIN", "LATDTMIN", "LONDTMAX", "LATDTMAX"],
            ]
            .to_numpy()
            .T
        )

        ALL_LONDTMIN.extend(list(LONDTMIN))
        ALL_LATDTMIN.extend(list(LATDTMIN))
        ALL_LONDTMAX.extend(list(LONDTMAX))
        ALL_LATDTMAX.extend(list(LATDTMAX))
        ALL_RAW_HARPS_TIMES.extend(list(cme_closest_harps_time_indices))

    final_database.loc[harps_indices, "HARPS_RAW_LONDTMIN"] = ALL_LONDTMIN
    final_database.loc[harps_indices, "HARPS_RAW_LATDTMIN"] = ALL_LATDTMIN
    final_database.loc[harps_indices, "HARPS_RAW_LONDTMAX"] = ALL_LONDTMAX
    final_database.loc[harps_indices, "HARPS_RAW_LATDTMAX"] = ALL_LATDTMAX
    final_database.loc[harps_indices, "HARPS_RAW_DATE"] = ALL_RAW_HARPS_TIMES

    final_database["HARPS_DATE"] = None
    final_database["HARPS_MIDPOINT"] = None
    final_database["HARPS_DISTANCE_TO_SUN_CENTRE"] = None
    final_database["HARPS_PA"] = None
    final_database["CME_HARPS_PA_DIFF"] = None
    final_database["HARPS_RAW_BBOX"] = None
    final_database["HARPS_LONDTMIN"] = None
    final_database["HARPS_LATDTMIN"] = None
    final_database["HARPS_LONDTMAX"] = None
    final_database["HARPS_LATDTMAX"] = None

    return final_database


def findSpatialCoOcurrentHarps(cme_ids):
    return_database = final_database[[ID in cme_ids for ID in final_database["CME_ID"]]]
    cme_grouped_matching_harps = return_database.groupby("CME_ID")

    for cme_id, group in tqdm(cme_grouped_matching_harps):
        CME_DETECTION_DATE = group.iloc[0]["CME_DATE"]
        CME_PA = group.iloc[0]["CME_PA"]
        CME_WIDTH = group.iloc[0]["CME_WIDTH"]
        CME_IS_HALO = bool(group.iloc[0]["CME_HALO"])
        CME_SEEN_ONLY_IN = int(group.iloc[0]["CME_SEEN_IN"])

        cme = CME(
            CME_DETECTION_DATE,
            CME_PA,
            CME_WIDTH,
            halo=CME_IS_HALO,
            seen_only_in=CME_SEEN_ONLY_IN,
        )

        for idx, harps_data in group.iterrows():
            HARPS_DATE = harps_data["HARPS_RAW_DATE"]
            LONDTMIN = harps_data["HARPS_RAW_LONDTMIN"]
            LATDTMIN = harps_data["HARPS_RAW_LATDTMIN"]
            LONDTMAX = harps_data["HARPS_RAW_LONDTMAX"]
            LATDTMAX = harps_data["HARPS_RAW_LATDTMAX"]
            HARPNUM = harps_data["HARPNUM"]

            harps = Harps(
                date=HARPS_DATE,
                lon_min=LONDTMIN,
                lat_min=LATDTMIN,
                lon_max=LONDTMAX,
                lat_max=LATDTMAX,
                HARPNUM=HARPNUM,
            )
            if np.abs(HARPS_DATE - CME_DETECTION_DATE) > 12 * u.min:
                try:
                    harps = harps.rotate_bbox(
                        CME_DETECTION_DATE
                    )  # Rotate if no timestamp within 12 minutes
                except TypeError:
                    harps = harps
                except InvalidBoundingBox:
                    harps = harps

            #            return_database.at[harps_data.index, "HARPS_SPAT_CONSIST"] = is_harps_within_boundary
            return_database.at[idx, "HARPS_DATE"] = harps.DATE.to_string()
            return_database.at[
                idx, "HARPS_MIDPOINT"
            ] = harps.get_centre_point().get_raw_coords()
            return_database.at[
                idx, "HARPS_DISTANCE_TO_SUN_CENTRE"
            ] = harps.get_distance_to_sun_centre()
            return_database.at[idx, "HARPS_PA"] = harps.get_position_angle()
            return_database.at[idx, "CME_HARPS_PA_DIFF"] = cme.get_bbox_pa_diff(harps)
            return_database.at[idx, "HARPS_RAW_BBOX"] = harps.get_raw_bbox()
            return_database.at[idx, "HARPS_LONDTMIN"] = harps.get_raw_bbox()[0][0]
            return_database.at[idx, "HARPS_LATDTMIN"] = harps.get_raw_bbox()[0][1]
            return_database.at[idx, "HARPS_LONDTMAX"] = harps.get_raw_bbox()[1][0]
            return_database.at[idx, "HARPS_LATDTMAX"] = harps.get_raw_bbox()[1][1]
    return return_database


def find_matches_and_save(final_database):
    non_halo = final_database[final_database["CME_HALO"] == 0]
    halo = final_database[final_database["CME_HALO"] == 1]

    non_halo_matching = non_halo["CME_HARPS_PA_DIFF"] < (
        non_halo["CME_WIDTH"] / 2 + EXTRA_CME_WIDTH
    )
    halo_matching = halo["HARPS_DISTANCE_TO_SUN_CENTRE"] < HALO_MAX_SUN_CENTRE_DIST

    matches = pd.concat([non_halo_matching, halo_matching], axis=0, sort=True)

    final_database = pd.concat(
        [final_database, matches.rename("HARPS_SPAT_CONSIST")], axis=1
    )

    final_database.sort_values(by=["CME_DATE", "HARPNUM"], inplace=True)

    #    final_database.to_csv(ALL_MATCHING_HARPS_DATABASE, index=False)
    #    final_database.to_pickle(ALL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, index=False)
    final_database.to_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

    final_database.to_csv(MAIN_DATABASE, index=False)
    final_database.to_pickle(MAIN_DATABASE_PICKLE)


if __name__ == "__main__":
    clear_screen()
    N = 4

    final_database = setup()

    clear_screen()

    final_database_copy = final_database.copy()

    cme_ids_all = np.array(list(set(final_database["CME_ID"])))

    cme_ids_list = np.array_split(cme_ids_all, N)

    print("\n===Finding Spatially Matching Harps.===\n")
    print("\n=Rotating Harps Positions=\n")
    with mp.Pool(processes=N) as pool:
        final_database_copy = pd.concat(
            list(tqdm(pool.imap(findSpatialCoOcurrentHarps, cme_ids_list)))
        )

    find_matches_and_save(final_database_copy)

    clear_screen()
