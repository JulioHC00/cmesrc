import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from src.harps.harps import Harps
from src.dimmings.dimmings import Dimming, OffDiskDimming
from src.cmesrc.utils import get_closest_harps_timestamp
from os.path import join
from os import walk
import numpy as np
from src.cmesrc.config import SWAN_DATA_DIR, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, RAW_DIMMINGS_CATALOGUE, HARPS_MATCHING_DIMMINGS_DATABASE, HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE

MAX_DIST = 10 / 360 * np.pi
MIN_R_OFF_DISK = 0.8
MAX_ANG_DIST_OFF_DISK = 20

raw_dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)
raw_dimmings_catalogue.set_index("dimming_id", inplace=True, drop=False)
raw_dimmings_catalogue["max_detection_time"] = Time(raw_dimmings_catalogue["max_detection_time"].to_list(), format="iso")

for dimming_id, dimming_data in raw_dimmings_catalogue.iterrows():
    if dimming_data["avg_r"] >= 1:
        continue
    if np.any(pd.isna(dimming_data[["longitude", "latitude"]])):
        x = dimming_data["avg_x"]
        y = dimming_data["avg_y"]

        lat = np.arcsin(y)
        lon = np.arcsin(x / np.cos(np.arcsin(y)))

        lat = lat * 180 / np.pi
        lon = lon * 180 / np.pi

        raw_dimmings_catalogue.at[dimming_id, "longitude"] = lon
        raw_dimmings_catalogue.at[dimming_id, "latitude"] = lat
    

on_disk_mask = raw_dimmings_catalogue["avg_r"] < 1
on_disk_dimmings = raw_dimmings_catalogue[on_disk_mask]

temporal_dimmings_matches = pd.read_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)
temporal_dimmings_matches.set_index("CME_HARPNUM_DIMMING_ID", inplace=True, drop=False)

final_df = temporal_dimmings_matches.copy()

harps_classification = []
dimmings = dict()
for idx, data in tqdm(temporal_dimmings_matches.iterrows(), total=temporal_dimmings_matches.shape[0]):
    matching_dimming_id = data["DIMMING_ID"]

    cme_pa = data["CME_PA"]
    cme_width = data["CME_WIDTH"]
    cme_time = data["CME_DATE"]

    matching_dimming_data = raw_dimmings_catalogue.loc[matching_dimming_id]

    if matching_dimming_id not in dimmings.keys():
        dimming_time = matching_dimming_data["max_detection_time"]
        dimming_lon = matching_dimming_data["longitude"]
        dimming_lat = matching_dimming_data["latitude"]

        dimming = Dimming(
                date = dimming_time,
                lon = dimming_lon,
                lat = dimming_lat
                )

        dimmings[matching_dimming_id] = dimming


    harps_data = data[["HARPS_DATE", "HARPS_LON_MIN", "HARPS_LAT_MIN", "HARPS_LON_MAX", "HARPS_LAT_MAX", "HARPNUM"]].to_list()

    harps = Harps(*harps_data)

    dimming = dimmings[matching_dimming_id]

    rotated_dimming_point = dimming.point.rotate_coords(harps.DATE)

    harps_dimming_dist = harps.get_spherical_point_distance(rotated_dimming_point)

    final_df.at[idx, f"DIMMING_TIME"] = dimming.point.DATE
    final_df.at[idx, f"DIMMING_CME_TIME_DIFF"] = np.abs(data["CME_DATE"] - dimming.point.DATE)
    final_df.at[idx, f"DIMMING_CME_PA_DIFF"] = np.abs(data["CME_PA"] - rotated_dimming_point.get_position_angle())
    final_df.at[idx, f"DIMMING_HARPS_DIST"] = harps_dimming_dist
    final_df.at[idx, f"DIMMING_HARPS_PA_DIFF"] = np.abs(data["HARPS_PA"] - rotated_dimming_point.get_position_angle())
    final_df.at[idx, f"DIMMING_HARP_R_DIFF"] = np.abs(harps.get_distance_to_sun_centre() - rotated_dimming_point.get_distance_to_sun_centre())
    final_df.at[idx, f"DIMMING_R"] = rotated_dimming_point.get_distance_to_sun_centre()
    final_df.at[idx, f"DIMMING_X"] = rotated_dimming_point.get_cartesian_coords()[0]
    final_df.at[idx, f"DIMMING_Y"] = rotated_dimming_point.get_cartesian_coords()[1]
    final_df.at[idx, f"DIMMING_LON"] = rotated_dimming_point.LON
    final_df.at[idx, f"DIMMING_LAT"] = rotated_dimming_point.LAT
    final_df.at[idx, f"DIMMING_PA"] = rotated_dimming_point.get_position_angle()

final_df.to_csv(HARPS_MATCHING_DIMMINGS_DATABASE, index=False)
final_df.to_pickle(HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)
