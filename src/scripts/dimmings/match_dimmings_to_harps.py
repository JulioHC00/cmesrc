import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from src.harps.harps import Harps
from src.dimmings.dimmings import Dimming
from src.cmesrc.utils import (
    clear_screen,
    get_closest_harps_timestamp,
    read_sql_processed_bbox,
)
import numpy as np
from src.cmesrc.config import (
    RAW_DIMMINGS_CATALOGUE,
    DIMMINGS_MATCHED_TO_HARPS,
    DIMMINGS_MATCHED_TO_HARPS_PICKLE,
    CMESRCV3_DB,
)

import sqlite3
from bisect import bisect_right

conn = sqlite3.connect(CMESRCV3_DB)
cur = conn.cursor()

DEG_TO_RAD = np.pi / 180
HALF_POINTS_DIST = 5 * DEG_TO_RAD
NO_POINTS_DIST = 10 * DEG_TO_RAD


def gather_dimming_distances():
    clear_screen()

    print("===DIMMINGS===")
    print("==Matching dimmings to HARPs==")

    ################################
    # Read in the dimmings catalogue
    ################################

    raw_dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)
    raw_dimmings_catalogue.set_index("dimming_id", inplace=True, drop=False)
    raw_dimmings_catalogue["max_detection_time"] = Time(
        raw_dimmings_catalogue["max_detection_time"].to_list()
    )

    # ALERT: Dropping dimmings that are missing longitude or latitude for now

    raw_dimmings_catalogue.dropna(subset=["longitude", "latitude"], inplace=True)

    ################################

    ##################################
    # Read in HARPs lifetime catalogue
    ##################################

    harps_lifetime_database = pd.read_sql(
        """
                                        SELECT * FROM HARPS
                                        WHERE harpnum IN (SELECT DISTINCT harpnum FROM PROCESSED_HARPS_BBOX)
                                        """,
        conn,
    )

    harps_lifetime_start_times = pd.to_datetime(
        harps_lifetime_database["start"]
    ).to_numpy()
    harps_lifetime_end_times = pd.to_datetime(harps_lifetime_database["end"]).to_numpy()
    harpsnums = harps_lifetime_database["harpnum"].to_numpy()

    # We need to sort both start and end times because we will perform a binary
    # search But we also need to keep track of the original indices so we can
    # extract the proper HARPS regions

    sorted_start_indices = np.argsort(harps_lifetime_start_times)
    sorted_end_indices = np.argsort(harps_lifetime_end_times)

    harps_lifetime_start_times_sorted = harps_lifetime_start_times[sorted_start_indices]
    harps_lifetime_end_times_sorted = harps_lifetime_end_times[sorted_end_indices]

    ##################################

    #######################################################
    # Find harps regions present at the time of the dimming
    #######################################################

    clear_screen()

    print("===DIMMINGS===")
    print("==Finding HARPs present at dimming time==")

    full_list_of_matches = []
    new_data_rows = []

    for dimming_id, dimming_data in tqdm(
        raw_dimmings_catalogue.iterrows(), total=raw_dimmings_catalogue.shape[0]
    ):
        dimming_time = dimming_data["max_detection_time"]

        start_index = bisect_right(harps_lifetime_start_times_sorted, dimming_time)
        end_index = bisect_right(harps_lifetime_end_times_sorted, dimming_time)

        start_indices = sorted_start_indices[:start_index]
        end_indices = sorted_end_indices[end_index:]

        harps = harpsnums[np.intersect1d(start_indices, end_indices)]

        full_list_of_matches.append((dimming_id, list(harps)))

    for i, (dimming_id, harpnum_list) in enumerate(full_list_of_matches):
        for harpnum in harpnum_list:
            new_row = raw_dimmings_catalogue.loc[dimming_id].to_dict()
            new_row["HARPNUM"] = harpnum
            new_row["DIMMING_HARPNUM_ID"] = f"ID{dimming_id}{harpnum}"
            new_data_rows.append(new_row)

    dimmings_harps_df = pd.DataFrame.from_records(new_data_rows)

    #######################################################

    ##########################################################################
    # Now we need to calculate the distances between the dimming and the HARPs
    ##########################################################################

    # This will be similar to the spatial matching of the CMEs, at least in the
    # beginning

    clear_screen()

    print("===DIMMINGS===")
    print("==Getting closest timestamp for HARPs==")

    # Group by HARPNUM

    grouped_by_harps = dimmings_harps_df.groupby("HARPNUM")

    harps_indices = None
    ALL_LONDTMIN = []
    ALL_LATDTMIN = []
    ALL_LONDTMAX = []
    ALL_LATDTMAX = []
    ALL_RAW_HARPS_TIMES = []

    for harpnum, group in tqdm(grouped_by_harps, total=grouped_by_harps.ngroups):
        harps_data = read_sql_processed_bbox(harpnum, conn)
        harps_timestamps = harps_data["Timestamp"].to_numpy()

        dimming_times = group["max_detection_time"].to_numpy()

        if harps_indices is None:
            harps_indices = group.index
        else:
            harps_indices = harps_indices.append(group.index)

        dimmings_closest_time_indices = []

        for dimming_time in dimming_times:
            closest_time_index = get_closest_harps_timestamp(
                harps_timestamps, dimming_time
            )
            dimmings_closest_time_indices.append(closest_time_index)

        LONDTMIN, LATDTMIN, LONDTMAX, LATDTMAX = (
            harps_data.loc[
                dimmings_closest_time_indices,
                ["LONDTMIN", "LATDTMIN", "LONDTMAX", "LATDTMAX"],
            ]
            .to_numpy()
            .T
        )

        ALL_LONDTMIN.extend(list(LONDTMIN))
        ALL_LATDTMIN.extend(list(LATDTMIN))
        ALL_LONDTMAX.extend(list(LONDTMAX))
        ALL_LATDTMAX.extend(list(LATDTMAX))
        ALL_RAW_HARPS_TIMES.extend(list(dimmings_closest_time_indices))

    dimmings_harps_df.at[harps_indices, "HARPS_RAW_LONDTMIN"] = ALL_LONDTMIN
    dimmings_harps_df.at[harps_indices, "HARPS_RAW_LATDTMIN"] = ALL_LATDTMIN
    dimmings_harps_df.at[harps_indices, "HARPS_RAW_LONDTMAX"] = ALL_LONDTMAX
    dimmings_harps_df.at[harps_indices, "HARPS_RAW_LATDTMAX"] = ALL_LATDTMAX
    dimmings_harps_df.at[harps_indices, "HARPS_RAW_DATE"] = ALL_RAW_HARPS_TIMES

    ##########################################################################

    ####################################
    # And now we calculate the distances
    ####################################

    clear_screen()

    print("===DIMMINGS===")
    print("==Rotating bounding boxes and calculating distances==")

    for idx, data in tqdm(
        dimmings_harps_df.iterrows(), total=dimmings_harps_df.shape[0]
    ):
        dimming_data = data[["max_detection_time", "longitude", "latitude"]]
        harps_data = data[
            [
                "HARPS_RAW_DATE",
                "HARPS_RAW_LONDTMIN",
                "HARPS_RAW_LATDTMIN",
                "HARPS_RAW_LONDTMAX",
                "HARPS_RAW_LATDTMAX",
                "HARPNUM",
            ]
        ].to_numpy()

        harps = Harps(*harps_data)
        dimming = Dimming(*dimming_data)

        harps_dimming_dist = harps.get_spherical_point_distance(dimming.point)

        dimmings_harps_df.at[idx, "HARPS_DIMMING_DISTANCE"] = harps_dimming_dist

    ##########################################
    # Now we calculate scores for the distance
    ##########################################

    clear_screen()

    print("===DIMMINGS===")
    print("==Scoring distances and matching dimmings to HARPs==")

    distances = dimmings_harps_df["HARPS_DIMMING_DISTANCE"].to_numpy()

    dimmings_harps_df["POSITION_SCORES"] = np.piecewise(
        distances,
        [distances <= NO_POINTS_DIST, distances > NO_POINTS_DIST],
        [
            lambda x: 100 * np.exp(-(np.log(2) / HALF_POINTS_DIST**2) * x**2),
            lambda x: 0,
        ],
    )

    unmatched_dimmings = 0
    matched_dimmings = 0

    scored_data = dimmings_harps_df.copy()

    for dimming_id, group in tqdm(dimmings_harps_df.groupby("dimming_id")):
        filtered_group = group[group["POSITION_SCORES"] > 0].copy()

        scored_data.loc[group.index, "MATCH"] = False

        if len(filtered_group) == 0:
            unmatched_dimmings += 1
            continue

        filtered_group.sort_values(
            by=["POSITION_SCORES"], ascending=[False], inplace=True
        )

        matching_index = filtered_group.index[0]

        scored_data.loc[matching_index, "MATCH"] = True

        matched_dimmings += 1

    clear_screen()

    print(f"MATCHED DIMMINGS: {matched_dimmings}")
    print(f"UNMATCHED DIMMINGS: {unmatched_dimmings}")

    scored_data.sort_values(
        by=["dimming_id", "POSITION_SCORES"], ascending=[True, False], inplace=True
    )

    # Test before saving

    matched_data = scored_data[scored_data["MATCH"]]

    duplicate_matches = matched_data.duplicated(
        subset=["dimming_id", "MATCH"], keep=False
    )

    if np.any(duplicate_matches):
        print(scored_data[duplicate_matches])
        raise ValueError("Duplicate matches found")

    scored_data.to_csv(DIMMINGS_MATCHED_TO_HARPS, index=False)
    scored_data.to_pickle(DIMMINGS_MATCHED_TO_HARPS_PICKLE)


if __name__ == "__main__":
    clear_screen()

    gather_dimming_distances()

    clear_screen()
