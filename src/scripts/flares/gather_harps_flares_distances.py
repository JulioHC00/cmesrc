import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from src.harps.harps import Harps
from src.flares.flares import Flare
import numpy as np
from src.cmesrc.config import TEMPORAL_MATCHING_FLARES_DATABASE_PICKLE, RAW_FLARE_CATALOGUE, HARPS_MATCHING_FLARES_DATABASE, HARPS_MATCHING_FLARES_DATABASE_PICKLE
from src.cmesrc.utils import clear_screen

def gather_flare_distances():
    print("===FLARES===")
    print("==Calculating flare-harps distances==")
    raw_flares_catalogue = pd.read_csv(RAW_FLARE_CATALOGUE)
    raw_flares_catalogue.set_index("hec_id", inplace=True, drop=False)
    raw_flares_catalogue["time_peak"] = Time(raw_flares_catalogue["time_peak"].to_list(), format="iso")

    temporal_flares_matches = pd.read_pickle(TEMPORAL_MATCHING_FLARES_DATABASE_PICKLE)
    temporal_flares_matches.set_index("CME_HARPNUM_FLARE_ID", inplace=True, drop=False)

    final_df = temporal_flares_matches.copy()

    harps_classification = []
    flares = dict()

    for idx, data in tqdm(temporal_flares_matches.iterrows(), total=temporal_flares_matches.shape[0]):
        matching_flare_id = data["FLARE_ID"]

        cme_pa = data["CME_PA"]
        cme_width = data["CME_WIDTH"]
        cme_time = data["CME_DATE"]

        matching_flare_data = raw_flares_catalogue.loc[matching_flare_id]

        if matching_flare_id not in flares.keys():
            flare_time = matching_flare_data["time_peak"]
            flare_lon = matching_flare_data["long_hg"]
            flare_lat = matching_flare_data["lat_hg"]
            flare_class = matching_flare_data["xray_class"]

            flare = Flare(
                    date = flare_time,
                    lon = flare_lon,
                    lat = flare_lat,
                    xr_class = flare_class
                    )

            flares[matching_flare_id] = flare


        harps_data = data[["HARPS_DATE", "HARPS_LON_MIN", "HARPS_LAT_MIN", "HARPS_LON_MAX", "HARPS_LAT_MAX", "HARPNUM"]].to_list()

        harps = Harps(*harps_data)

        flare = flares[matching_flare_id]

        rotated_flare_point = flare.point.rotate_coords(harps.DATE)


        harps_flare_dist = harps.get_spherical_point_distance(rotated_flare_point)


        final_df.at[idx, f"FLARE_TIME"] = flare.point.DATE
        final_df.at[idx, f"FLARE_CME_TIME_DIFF"] = np.abs(data["CME_DATE"] - flare.point.DATE)
        final_df.at[idx, f"FLARE_CME_PA_DIFF"] = np.abs(data["CME_PA"] - rotated_flare_point.get_position_angle())
        final_df.at[idx, f"FLARE_HARPS_DIST"] = harps_flare_dist
        final_df.at[idx, f"FLARE_HARPS_PA_DIFF"] = np.abs(data["HARPS_PA"] - rotated_flare_point.get_position_angle())
        final_df.at[idx, f"FLARE_HARP_R_DIFF"] = np.abs(harps.get_distance_to_sun_centre() - rotated_flare_point.get_distance_to_sun_centre())
        final_df.at[idx, f"FLARE_R"] = rotated_flare_point.get_distance_to_sun_centre()
        final_df.at[idx, f"FLARE_X"] = rotated_flare_point.get_cartesian_coords()[0]
        final_df.at[idx, f"FLARE_Y"] = rotated_flare_point.get_cartesian_coords()[1]
        final_df.at[idx, f"FLARE_LON"] = rotated_flare_point.LON
        final_df.at[idx, f"FLARE_LAT"] = rotated_flare_point.LAT
        final_df.at[idx, f"FLARE_PA"] = rotated_flare_point.get_position_angle()
        final_df.at[idx, f"FLARE_CLASS"] = flare.XR_CLASS

    final_df.to_csv(HARPS_MATCHING_FLARES_DATABASE, index=False)
    final_df.to_pickle(HARPS_MATCHING_FLARES_DATABASE_PICKLE)

if __name__ == "__main__":
    clear_screen()

    gather_flare_distances()

    clear_screen()
