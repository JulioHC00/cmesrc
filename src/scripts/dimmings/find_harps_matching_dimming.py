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

def cacheSwanData() -> dict:
    print("\n==CACHING SWAN DATA.==\n")
    data_dict = dict()

    for directoryName, subdirectoryName, fileList in walk(SWAN_DATA_DIR):
        for fileName in tqdm(fileList):
            harpnum = int(fileName.split('.')[0])

            df = pd.read_csv(join(directoryName, fileName), sep="\t", na_values="None", usecols=['Timestamp', 'LAT_MIN', 'LON_MIN', 'LAT_MAX', 'LON_MAX']).dropna()

            timestamps = list(df["Timestamp"].to_numpy())

            df['Timestamp'] = Time(timestamps, format="iso")

            df.set_index("Timestamp", drop=False, inplace=True)

            data_dict[harpnum] = df

    return data_dict


raw_dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)
raw_dimmings_catalogue.set_index("dimming_id", inplace=True)
raw_dimmings_catalogue["off_disk"] = raw_dimmings_catalogue["longitude"].isna()
raw_dimmings_catalogue["max_detection_time"] = Time(raw_dimmings_catalogue["max_detection_time"].to_list(), format="iso")

temporal_dimmings_matches = pd.read_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)
temporal_dimmings_matches.set_index("cme_id", inplace=True)
spatiotemporal_matching_harps = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
spatiotemporal_matching_harps.set_index("cme_harps_id", inplace=True)

SWAN_DATA = cacheSwanData()

harps_classification = []
for cme_id, group in tqdm(spatiotemporal_matching_harps.groupby("cme_id")):
    matching_dimming_ids = temporal_dimmings_matches.loc[cme_id, "dimming_ids"]
    cme_pa = group["pa"].to_numpy()[0]
    cme_width = group["width"].to_numpy()[0]
    cme_time = group["date"].to_numpy()[0]

    if len(matching_dimming_ids) == 0:
        continue

    matching_dimmings = raw_dimmings_catalogue.loc[matching_dimming_ids]


    for dimming_id, dimming_data in matching_dimmings.iterrows():
        dimming_time = dimming_data["max_detection_time"]

        closest_harps_time = { 
                              harpnum : get_closest_harps_timestamp(SWAN_DATA[harpnum]["Timestamp"].to_numpy(), dimming_time) for harpnum in group["harpnum"].to_numpy()
                              }

        harps_list = []

        for harpnum in group["harpnum"].to_numpy():
            data = SWAN_DATA[harpnum].loc[closest_harps_time[harpnum]][["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()
            data.append(harpnum)
            harps_list.append(Harps(*data))


        if dimming_data["off_disk"]:
            dimming_x = dimming_data["avg_x"]
            dimming_y = dimming_data["avg_y"]
            dimming = OffDiskDimming(
                    date = dimming_time,
                    x = dimming_x,
                    y = dimming_y
                    )

            for harps in harps_list:
                ang_dist = dimming.get_ang_dist_harps(harps)
                r_dist = np.abs(dimming.R - harps.get_distance_to_sun_centre())

                new_row = {
                        "dimming_id" : dimming_id,
                        "cme_id" : cme_id,
                        "cme_pa" : cme_pa,
                        "cme_width" : cme_width,
                        "cme_time" : cme_time,
                        "cme_harps_dimming_id" : f"{cme_id}.{harps.HARPNUM}.{dimming_id}",
                        "harpnum" : harps.HARPNUM,
                        "dimming_time" : dimming_time,
                        "dimming_x" : dimming_x,
                        "dimming_y" : dimming_y,
                        "dimming_r" : dimming.R,
                        "dimming_pa" : dimming.getPA(),
                        "ang_dist" : ang_dist,
                        "r_dist" : r_dist,
                        "off_disk_dimming" : True,
                        "harps_pa" : harps.get_position_angle(),
                        "harps_bbox" : harps.get_raw_bbox(),
                        "harps_r" : harps.get_distance_to_sun_centre()
                        }

                harps_classification.append(new_row)

        else:
            dimming_lon = dimming_data["longitude"]
            dimming_lat = dimming_data["latitude"]

            dimming = Dimming(
                    date = dimming_time,
                    lon = dimming_lon,
                    lat = dimming_lat
                    )

            for harps in harps_list:
                abs_dist = harps.get_spherical_point_distance(dimming.point)
                r_dist = np.abs(dimming.point.get_distance_to_sun_centre() - harps.get_distance_to_sun_centre())

                new_row = {
                        "dimming_id" : dimming_id,
                        "cme_id" : cme_id,
                        "cme_harps_dimming_id" : f"{cme_id}.{harps.HARPNUM}.{dimming_id}",
                        "cme_id" : cme_id,
                        "cme_pa" : cme_pa,
                        "cme_width" : cme_width,
                        "cme_time" : cme_time,
                        "harpnum" : harps.HARPNUM,
                        "dimming_time" : dimming_time,
                        "dimming_lon" : dimming_lon,
                        "dimming_lat" : dimming_lat,
                        "dimming_r" : dimming.point.get_distance_to_sun_centre(),
                        "dimming_pa" : dimming.point.get_position_angle(),
                        "abs_dist" : abs_dist,
                        "r_dist" : r_dist,
                        "off_disk_dimming" : False,
                        "harps_bbox" : harps.get_raw_bbox(),
                        "harps_pa" : harps.get_position_angle(),
                        "harps_r" : harps.get_distance_to_sun_centre()
                        }

                harps_classification.append(new_row)

classification_df = pd.DataFrame.from_records(harps_classification)

for idx, match in classification_df.iterrows():
    if match["off_disk_dimming"]:
        if (match["ang_dist"] < MAX_ANG_DIST_OFF_DISK) and (match["harps_r"] > MIN_R_OFF_DISK):
            classification_df.at[idx, "within_boundaries"] = True
        else:
            classification_df.at[idx, "within_boundaries"] = False
    else:
        if (match["abs_dist"] < MAX_DIST):
            classification_df.at[idx, "within_boundaries"] = True
        else:
            classification_df.at[idx, "within_boundaries"] = False

for cme_id, data in classification_df.groupby("cme_id"):
    accepted_data = data[data["within_boundaries"]]

    for dimming_id, dist_data in accepted_data.groupby("dimming_id"):
        if raw_dimmings_catalogue.loc[dimming_id]["off_disk"]:
            sorted_dist_data = dist_data.sort_values(by="ang_dist")
            match_index = sorted_dist_data.index[0]
            classification_df.at[match_index, "match"] = True
        else:
            sorted_dist_data = dist_data.sort_values(by="abs_dist")
            match_index = sorted_dist_data.index[0]
            classification_df.at[match_index, "match"] = True

    classification_df["match"] = classification_df["match"].fillna(False)


classification_df.to_csv(HARPS_MATCHING_DIMMINGS_DATABASE, index=False)
classification_df.to_pickle(HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)
