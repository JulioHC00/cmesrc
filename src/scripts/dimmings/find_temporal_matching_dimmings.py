from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, RAW_DIMMINGS_CATALOGUE, TEMPORAL_MATCHING_DIMMINGS_DATABASE, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE
import pandas as pd
from tqdm import tqdm
import astropy.units as u
from astropy.time import Time
from bisect import bisect_left
import numpy as np

def find_temporal_matching_dimmings():
    spatiotemporal_matching_harps_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    spatiotemporal_matching_harps_database.set_index("CME_HARPNUM_ID", drop=False, inplace=True)
    dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)

    on_disk_mask = dimmings_catalogue["avg_r"] < 1

    dimmings_catalogue = dimmings_catalogue[on_disk_mask]

    dimmings_catalogue["start_time"] = Time(dimmings_catalogue["start_time"].to_list())

    MAX_TIME_BACK_FROM_CME_TIME =  3 * u.hour
    MAX_TIME_FORWARD_FROM_CME_TIME =  0 * u.hour

    dimmings_start_times = dimmings_catalogue["start_time"].to_numpy()
    dimmings_ids = dimmings_catalogue["dimming_id"].to_numpy()

    matching_dimming_rows = []

    grouped_cmes = spatiotemporal_matching_harps_database.groupby(["CME_ID", "CME_DATE"])

    for cme_id_date, group in tqdm(grouped_cmes):
        cme_id, cme_date = cme_id_date

        minimum_time = cme_date - MAX_TIME_BACK_FROM_CME_TIME
        maximum_time = cme_date + MAX_TIME_FORWARD_FROM_CME_TIME

        low_idx = bisect_left(dimmings_start_times, minimum_time)
        high_idx = bisect_left(dimmings_start_times, maximum_time)

        matching_dimmings = dimmings_ids[low_idx:high_idx]

        for id in group.index:
            for i, dimming_id in enumerate(matching_dimmings):
                new_row = spatiotemporal_matching_harps_database.loc[id].copy()
                new_row.at["DIMMING_ID"] = dimming_id
                new_row.at["CME_HARPNUM_DIMMING_ID"] = id + str(dimming_id)
                matching_dimming_rows.append(new_row)

    matching_dimmings_df = pd.DataFrame.from_records(matching_dimming_rows)
    matching_dimmings_df.to_csv(TEMPORAL_MATCHING_DIMMINGS_DATABASE, index=False)
    matching_dimmings_df.to_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)

if __name__ == "__main__":
    find_temporal_matching_dimmings()
