"""
Match temporally co-occurent HARPS regions to CMEs
"""
from src.cmesrc.config import SWAN_DATA_DIR, TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE
from src.cmesrc.utils import parse_pandas_str_list, get_closest_harps_timestamp
from src.cmes.cmes import CME, MissmatchInTimes
from src.harps.harps import Harps
from os import walk
from os.path import join
from astropy.time import Time
from tqdm import tqdm
import numpy as np
import pandas as pd
import astropy.units as u

MAX_TIME_DIFF = 24 * u.min

rows = []

def cacheSwanData() -> dict:
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t", na_values="None", usecols=['Timestamp', 'LAT_MIN', 'LON_MIN', 'LAT_MAX', 'LON_MAX']).dropna()

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            data_dict[harpnum] = df

    return data_dict

def findSpatialCoOcurrentHarps():
    TEMPORAL_MATCHING_HARPS_DF = pd.read_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

    SWAN_DATA = cacheSwanData()

    final_database_rows = []

    print("\n==Finding Spatially Matching Harps.==\n")
    for idx, cme_data in tqdm(TEMPORAL_MATCHING_HARPS_DF.iterrows(), total=TEMPORAL_MATCHING_HARPS_DF.shape[0]):
        CME_DETECTION_DATE = cme_data["date"]
        CME_PA = cme_data["pa"]
        CME_WIDTH = cme_data["width"]
        CME_LINEAR_SPEED = cme_data["linear_speed"]
        CME_IS_HALO = bool(cme_data["halo"])
        CME_SEEN_ONLY_IN = int(cme_data["seen_in"])

        cme = CME(CME_DETECTION_DATE, CME_PA, CME_WIDTH, CME_LINEAR_SPEED, halo=CME_IS_HALO, seen_only_in = CME_SEEN_ONLY_IN)


        for harpnum in cme_data["matching_harps"]:
            harps_data = SWAN_DATA[harpnum]

            harps_timestamps = harps_data["Timestamp"].to_numpy()

            CME_CLOSEST_HARPS_TIME = get_closest_harps_timestamp(harps_timestamps, cme.LINEAR_TIME_AT_SUN_CENTER)

            harps_data_mask = (harps_timestamps == CME_CLOSEST_HARPS_TIME)

            LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = harps_data[harps_data_mask][
                    ["LON_MIN","LAT_MIN","LON_MAX","LAT_MAX"]
                    ].to_numpy()[0]

            HARPS_T_REC = harps_data[harps_data_mask]["Timestamp"].to_numpy()[0]


            harps = Harps(
                    date = HARPS_T_REC,
                    lon_min = LON_MIN,
                    lat_min = LAT_MIN,
                    lon_max = LON_MAX,
                    lat_max = LAT_MAX,
                    HARPNUM = harpnum
                          )

            try:
                is_harps_within_boundary, is_harps_rotated, harps_rotated_by, out_harps = cme.hasHarpsSpatialCoOcurrence(harps, max_time_diff=MAX_TIME_DIFF)
            except MissmatchInTimes as e:
                print(e.message)
                continue

            if is_harps_within_boundary:
                new_row = cme_data.drop(["matching_harps", "n_matching_harps"]).to_dict()
                new_row["cme_id"] = f"{new_row['id']}"
                new_row["cme_harps_id"] = f"{new_row['id']}.{out_harps.HARPNUM}"
                new_row["harpnum"] = out_harps.HARPNUM
                new_row["harps_T_REC"] = out_harps.DATE
                new_row["harps_raw_bbox"] = out_harps.get_raw_bbox()
                new_row["harps_LON_MIN"] = out_harps.get_raw_bbox()[0][0]
                new_row["harps_LAT_MIN"] = out_harps.get_raw_bbox()[0][1]
                new_row["harps_LON_MAX"] = out_harps.get_raw_bbox()[1][0]
                new_row["harps_LAT_MAX"] = out_harps.get_raw_bbox()[1][1]
                new_row["date"] = cme.DATE
                new_row["harps_pa"] = out_harps.get_position_angle()
                new_row["harps_midpoint"] = out_harps.get_centre_point().get_raw_coords()
                new_row["harps_distance_to_sun_centre"] = out_harps.get_distance_to_sun_centre()
                new_row["harps_rotated"] = int(is_harps_rotated)
                new_row["harps_rotated_by"] = harps_rotated_by

                final_database_rows.append(new_row)

    final_database = pd.DataFrame.from_records(final_database_rows)
    final_database.to_csv(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE, index=False)
    final_database.to_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

if __name__ == "__main__":
    findSpatialCoOcurrentHarps()
