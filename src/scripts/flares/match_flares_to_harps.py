import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from src.harps.harps import Harps
from src.flares.flares import Flare
from src.cmesrc.utils import clear_screen, cache_swan_data, get_closest_harps_timestamp
import numpy as np
from src.cmesrc.config import (
    RAW_FLARE_CATALOGUE,
    HARPS_LIFETIME_DATABSE, 
    FLARES_MATCHED_TO_HARPS, 
    FLARES_MATCHED_TO_HARPS_PICKLE
    )

from bisect import bisect_right

DEG_TO_RAD = np.pi / 180
HALF_POINTS_DIST = 10 * DEG_TO_RAD
NO_POINTS_DIST = 15 * DEG_TO_RAD

def flare_class_to_number(fclass):
    class_letters = {
        "A": 0,
        "B": 10,
        "C": 20,
        "M": 30,
        "X": 40,
    }
    letter = fclass[0]

    points = class_letters[letter]

    points += float(fclass[1:])

    return points

def gather_flare_distances():
    clear_screen()

    print("===FLARES===")
    print("==Matching flares to HARPs==")

    ################################
    # Read in the flares catalogue
    ################################

    raw_flares_catalogue = pd.read_csv(RAW_FLARE_CATALOGUE)
    raw_flares_catalogue.set_index("hec_id", inplace=True, drop=False)
    raw_flares_catalogue["time_peak"] = Time(raw_flares_catalogue["time_peak"].to_list())

    # ALERT: Dropping flares that are missing longitude or latitude

    raw_flares_catalogue.dropna(subset=["lat_hg", "long_hg"], inplace=True)

    ################################


    ##################################
    # Read in HARPs lifetime catalogue
    ##################################

    harps_lifetime_database = pd.read_csv(HARPS_LIFETIME_DATABSE)
    harps_lifetime_start_times = pd.to_datetime(harps_lifetime_database["start"]).to_numpy()
    harps_lifetime_end_times = pd.to_datetime(harps_lifetime_database["end"]).to_numpy()
    harpsnums = harps_lifetime_database["harpsnum"].to_numpy()

    # We need to sort both start and end times because we will perform a binary
    # search But we also need to keep track of the original indices so we can 
    # extract the proper HARPS regions

    sorted_start_indices = np.argsort(harps_lifetime_start_times)
    sorted_end_indices = np.argsort(harps_lifetime_end_times)

    harps_lifetime_start_times_sorted = harps_lifetime_start_times[sorted_start_indices]
    harps_lifetime_end_times_sorted = harps_lifetime_end_times[sorted_end_indices]

    ##################################

    #######################################################
    # Find harps regions present at the time of the flare
    #######################################################

    clear_screen()

    print("===FLARES===")
    print("==Finding HARPs present at flare time==")

    full_list_of_matches = []
    new_data_rows = []

    for flare_id, flare_data in tqdm(raw_flares_catalogue.iterrows(), total=raw_flares_catalogue.shape[0]):
        flare_time = flare_data["time_peak"]

        start_index = bisect_right(harps_lifetime_start_times_sorted, flare_time)
        end_index = bisect_right(harps_lifetime_end_times_sorted, flare_time)

        start_indices = sorted_start_indices[:start_index]
        end_indices = sorted_end_indices[end_index:]

        harps = harpsnums[np.intersect1d(start_indices, end_indices)]

        full_list_of_matches.append((flare_id, list(harps)))
    
    for i, (flare_id, harpnum_list) in enumerate(full_list_of_matches):
        for harpnum in harpnum_list:
            new_row = raw_flares_catalogue.loc[flare_id].to_dict()
            new_row["HARPNUM"] = harpnum
            new_row["flare_HARPNUM_ID"] = f"ID{flare_id}{harpnum}"
            new_data_rows.append(new_row)

    flares_harps_df = pd.DataFrame.from_records(new_data_rows)

    #######################################################

    ##########################################################################
    # Now we need to calculate the distances between the flare and the HARPs
    ##########################################################################


    # This will be similar to the spatial matching of the CMEs, at least in the
    # beginning

    SWAN_DATA = cache_swan_data()

    clear_screen()

    print("===FLARES===")
    print("==Getting closest timestamp for HARPs==")

    # Group by HARPNUM

    grouped_by_harps = flares_harps_df.groupby("HARPNUM")

    harps_indices = None
    ALL_LON_MIN = []
    ALL_LAT_MIN = []
    ALL_LON_MAX = []
    ALL_LAT_MAX = []
    ALL_RAW_HARPS_TIMES = []

    for harpnum, group in tqdm(grouped_by_harps, total=grouped_by_harps.ngroups):
        harps_data = SWAN_DATA[harpnum]
        harps_timestamps = harps_data["Timestamp"].to_numpy()

        flares_times = group["time_peak"].to_numpy()

        if harps_indices is None:
            harps_indices = group.index
        else:
            harps_indices = harps_indices.append(group.index)

        flares_closest_time_indices = []

        for flare_time in flares_times:
            closest_time_index = get_closest_harps_timestamp(harps_timestamps, flare_time)
            flares_closest_time_indices.append(closest_time_index)

        LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = harps_data.loc[flares_closest_time_indices, ["LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]].to_numpy().T

        ALL_LON_MIN.extend(list(LON_MIN))
        ALL_LAT_MIN.extend(list(LAT_MIN))
        ALL_LON_MAX.extend(list(LON_MAX))
        ALL_LAT_MAX.extend(list(LAT_MAX))
        ALL_RAW_HARPS_TIMES.extend(list(flares_closest_time_indices))
    
    flares_harps_df.at[harps_indices, "HARPS_RAW_LON_MIN"] = ALL_LON_MIN
    flares_harps_df.at[harps_indices, "HARPS_RAW_LAT_MIN"] = ALL_LAT_MIN
    flares_harps_df.at[harps_indices, "HARPS_RAW_LON_MAX"] = ALL_LON_MAX
    flares_harps_df.at[harps_indices, "HARPS_RAW_LAT_MAX"] = ALL_LAT_MAX
    flares_harps_df.at[harps_indices, "HARPS_RAW_DATE"] = ALL_RAW_HARPS_TIMES

    del SWAN_DATA

    ##########################################################################

    ####################################
    # And now we calculate the distances
    ####################################

    clear_screen()

    print("===FLARES===")
    print("==Rotating bounding boxes and calculating distances==")

    for idx, data in tqdm(flares_harps_df.iterrows(), total=flares_harps_df.shape[0]):

        flare_data = data[["time_peak", "long_hg", "lat_hg", "xray_class"]]
        harps_data = data[["HARPS_RAW_DATE", "HARPS_RAW_LON_MIN", "HARPS_RAW_LAT_MIN", "HARPS_RAW_LON_MAX", "HARPS_RAW_LAT_MAX", "HARPNUM"]].to_numpy()

        harps = Harps(*harps_data)
        flare = Flare(*flare_data)

        harps_flare_dist = harps.get_spherical_point_distance(flare.point)

        flares_harps_df.at[idx, "HARPS_FLARE_DISTANCE"] = harps_flare_dist
        flares_harps_df.at[idx, "FLARE_CLASS"] = flare_class_to_number(flare.XR_CLASS)

    ##########################################
    # Now we calculate scores for the distance
    ##########################################

    clear_screen()

    print("===FLARES===")
    print("==Scoring distances and matching flares to HARPs==")

    distances = flares_harps_df["HARPS_FLARE_DISTANCE"].to_numpy()

    flares_harps_df["POSITION_SCORES"] = np.piecewise(
            distances,
            [distances <= NO_POINTS_DIST, distances > NO_POINTS_DIST],
            [
                lambda x : 100 * np.exp(- (np.log(2) / HALF_POINTS_DIST ** 2) * x ** 2),
                lambda x : 0
                ]
            )

    unmatched_flares = 0
    matched_flares = 0

    scored_data = flares_harps_df.copy()

    for flare_id, group in tqdm(flares_harps_df.groupby("hec_id")):
        filtered_group = group[group["POSITION_SCORES"] > 0].copy()

        scored_data.loc[group.index, "MATCH"] = False

        if len(filtered_group) == 0:
            unmatched_flares += 1
            continue

        filtered_group.sort_values(by=["POSITION_SCORES"], ascending=[False], inplace=True)

        matching_index = filtered_group.index[0]

        scored_data.loc[matching_index, "MATCH"] = True

        matched_flares += 1

    clear_screen()

    print(f"MATCHED flareS: {matched_flares}")
    print(f"UNMATCHED flareS: {unmatched_flares}")

    scored_data.sort_values(by=["hec_id", "POSITION_SCORES"], ascending=[True, False], inplace=True)

    scored_data.to_csv(FLARES_MATCHED_TO_HARPS, index=False)
    scored_data.to_pickle(FLARES_MATCHED_TO_HARPS_PICKLE)

if __name__ == "__main__":
    clear_screen()

    gather_flare_distances()

    clear_screen()
