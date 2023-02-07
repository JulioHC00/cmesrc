from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, RAW_DIMMINGS_CATALOGUE, TEMPORAL_MATCHING_DIMMINGS_DATABASE, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE
import pandas as pd
from tqdm import tqdm
import astropy.units as u
from astropy.time import Time
from bisect import bisect_left
import numpy as np

def find_temporal_matching_dimmings():
    spatiotemporal_matching_harps_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)


    dimmings_catalogue["start_time"] = Time(dimmings_catalogue["start_time"].to_list())

    MAX_TIME_BACK_FROM_CME_TIME_AT_SUN_CENTRE =  3 * u.hour
    MAX_TIME_FORWARD_FROM_CME_TIME_AT_SUN_CENTRE =  0 * u.hour

    full_cme_ids = spatiotemporal_matching_harps_database["cme_id"].to_list()
    cme_times = Time(list(set(spatiotemporal_matching_harps_database["date"].to_numpy())))
    cme_ids = set(full_cme_ids)
    dimmings_start_times = dimmings_catalogue["start_time"].to_numpy()
    dimmings_ids = dimmings_catalogue["dimming_id"].to_numpy()

    minimum_times = cme_times - MAX_TIME_BACK_FROM_CME_TIME_AT_SUN_CENTRE
    maximum_times = cme_times + MAX_TIME_FORWARD_FROM_CME_TIME_AT_SUN_CENTRE

    matching_dimming_rows = []

    grouped_cmes = spatiotemporal_matching_harps_database.groupby(["cme_id", "date"])

    for cme_id_date, group in tqdm(grouped_cmes):
        cme_id, cme_date = cme_id_date

        minimum_time = cme_date - MAX_TIME_BACK_FROM_CME_TIME_AT_SUN_CENTRE
        maximum_time = cme_date + MAX_TIME_FORWARD_FROM_CME_TIME_AT_SUN_CENTRE

        low_idx = bisect_left(dimmings_start_times, minimum_time)
        high_idx = bisect_left(dimmings_start_times, maximum_time)

        matching_dimmings = dimmings_ids[low_idx:high_idx]

        matching_dimming_rows.append({
            "cme_id": cme_id,
            "dimming_ids": matching_dimmings
                                      })

    matching_dimmings_df = pd.DataFrame.from_records(matching_dimming_rows)
    matching_dimmings_df.to_csv(TEMPORAL_MATCHING_DIMMINGS_DATABASE, index=False)
    matching_dimmings_df.to_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)

if __name__ == "__main__":
    find_temporal_matching_dimmings()
