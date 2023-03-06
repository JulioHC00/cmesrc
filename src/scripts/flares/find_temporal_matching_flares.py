from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, RAW_FLARE_CATALOGUE, TEMPORAL_MATCHING_FLARES_DATABASE, TEMPORAL_MATCHING_FLARES_DATABASE_PICKLE
import pandas as pd
from tqdm import tqdm
import astropy.units as u
from astropy.time import Time
from bisect import bisect_left
import numpy as np

def find_temporal_matching_flares():
    spatiotemporal_matching_harps_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    spatiotemporal_matching_harps_database.set_index("CME_HARPNUM_ID", drop=False, inplace=True)
    flare_catalogue = pd.read_csv(RAW_FLARE_CATALOGUE)
    flare_catalogue.dropna(subset=["lat_hg", "long_hg"], inplace=True)

    flare_catalogue["time_peak"] = Time(flare_catalogue["time_peak"].to_list())

    MAX_TIME_BACK_FROM_CME_TIME =  3 * u.hour
    MAX_TIME_FORWARD_FROM_CME_TIME =  0 * u.hour

    flares_start_times = flare_catalogue["time_peak"].to_numpy()
    flares_ids = flare_catalogue["hec_id"].to_numpy()
    flares_ids = list(map(int, flares_ids))

    matching_flare_rows = []

    grouped_cmes = spatiotemporal_matching_harps_database.groupby(["CME_ID", "CME_DATE"])

    flare_ids = []
    indices = []

    for cme_id_date, group in tqdm(grouped_cmes):
        cme_id, cme_date = cme_id_date

        minimum_time = cme_date - MAX_TIME_BACK_FROM_CME_TIME
        maximum_time = cme_date + MAX_TIME_FORWARD_FROM_CME_TIME

        low_idx = bisect_left(flares_start_times, minimum_time)
        high_idx = bisect_left(flares_start_times, maximum_time)

        matching_flares = np.array(flares_ids[low_idx:high_idx])

        indices.extend(list(np.array([cme_id]).repeat(len(matching_flares))))

        flare_ids.extend(matching_flares)

    flares_series = pd.Series(index=indices, data=flare_ids).rename("FLARE_ID")
    matching_flares_df = spatiotemporal_matching_harps_database.copy()
    matching_flares_df = pd.merge(matching_flares_df, flares_series, left_on="CME_ID", right_index=True)
    matching_flares_df["CME_HARPNUM_FLARE_ID"] = [f"{cme_harpnum_id}{flare_id}" for cme_harpnum_id, flare_id in zip(matching_flares_df["CME_HARPNUM_ID"], matching_flares_df["FLARE_ID"])]
    matching_flares_df.to_csv(TEMPORAL_MATCHING_FLARES_DATABASE, index=False)
    matching_flares_df.to_pickle(TEMPORAL_MATCHING_FLARES_DATABASE_PICKLE)

if __name__ == "__main__":
    find_temporal_matching_flares()
