from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, RAW_DIMMINGS_CATALOGUE, TEMPORAL_MATCHING_DIMMINGS_DATABASE, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE
from src.cmesrc.utils import clear_screen
import pandas as pd
from tqdm import tqdm
import astropy.units as u
from astropy.time import Time
from bisect import bisect_left
import numpy as np

def find_temporal_matching_dimmings():
    print("===DIMMINGS===")
    print("==Finding temporal matching dimmings==")
    spatiotemporal_matching_harps_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    spatiotemporal_matching_harps_database.set_index("CME_HARPNUM_ID", drop=False, inplace=True)
    dimmings_catalogue = pd.read_csv(RAW_DIMMINGS_CATALOGUE)

    on_disk_mask = dimmings_catalogue["avg_r"] < 1

    dimmings_catalogue = dimmings_catalogue[on_disk_mask]

    dimmings_catalogue["start_time"] = Time(dimmings_catalogue["start_time"].to_list())

    # Need to make sure these are sorted

    dimmings_catalogue.sort_values(by="start_time", inplace=True)

    #######################################################################################
    # For now, need to remove all dimmings that don't have values for longitude or latitude
    #######################################################################################

    dimmings_catalogue.dropna(subset=["longitude", "latitude"], inplace=True)

    # More strict, Vrsnak et al. 2007 "Acceleration Phase of Coronal Mass
    # Ejections: I. Temporal and Spatial Scales" See an average speed of CMEs of
    # ~600 km/s. From parsed LASCO CME catalogue, 90% of the CMEs with measured
    # speeds have less than 700 km/s. So, supposing all CMEs are discovered in
    # LASCO C2 first, that corresponds to about 8 minuted between being launched
    # and it being seen in LASCO C2.  Of course, this is longer because there's
    # some acceleration. Slower CMEs, with speeds of around 100 km/s, should
    # take around 1 hour to be seen. So, with that in mind I think a window of
    # about 2 hours is sufficient.
    
    # AFTERTHOUGHT: While that is true if all CMEs are detected in C2, consider
    # one that is detected in C3, so that its detection time is after it
    # appeared in C3, then the time should be longer. In that case we should
    # allow for even longer thimes? So what if we just allow say 4 hours in
    # total
    
    # TODO: Change the times depending on where the CME was seen
    
    # Since it should also be around 8 minutes between the CME being launched
    # and it being seen in LASCO C2, I add a negative
    # MAX_TIMe_FORWARD_FROM_CME_TIME to account for that.
    
    # NOTE: LASCO C2 and C3 FOV from
    # https://www.sr.bham.ac.uk/solar/uls/lasco_.html
    
    # TODO: Possible improvement would be to use the speed of the CME to
    # determine the window size.  Problem is not all CMEs have a measured speed
    # so this wouldn't be applicable to all of them.

    MAX_TIME_BACK_FROM_CME_TIME =  4 * u.hour
    MAX_TIME_FORWARD_FROM_CME_TIME =  0 * u.hour

    dimmings_start_times = dimmings_catalogue["start_time"].to_numpy()
    dimmings_ids = dimmings_catalogue["dimming_id"].to_numpy()

    matching_dimming_rows = []

    grouped_cmes = spatiotemporal_matching_harps_database.groupby(["CME_ID", "CME_DATE"])

    dimming_ids = []
    indices = []

    for cme_id_date, group in tqdm(grouped_cmes):
        cme_id, cme_date = cme_id_date

        minimum_time = cme_date - MAX_TIME_BACK_FROM_CME_TIME
        maximum_time = cme_date + MAX_TIME_FORWARD_FROM_CME_TIME

        low_idx = bisect_left(dimmings_start_times, minimum_time)
        high_idx = bisect_left(dimmings_start_times, maximum_time)

        matching_dimmings = np.array(dimmings_ids[low_idx:high_idx])

        indices.extend(list(np.array([cme_id]).repeat(len(matching_dimmings))))

        dimming_ids.extend(matching_dimmings)

    dimmings_series = pd.Series(index=indices, data=dimming_ids).rename("DIMMING_ID")
    matching_dimmings_df = spatiotemporal_matching_harps_database.copy()
    matching_dimmings_df = pd.merge(matching_dimmings_df, dimmings_series, left_on="CME_ID", right_index=True)
    matching_dimmings_df["CME_HARPNUM_DIMMING_ID"] = [f"{cme_harpnum_id}{dimming_id}" for cme_harpnum_id, dimming_id in zip(matching_dimmings_df["CME_HARPNUM_ID"], matching_dimmings_df["DIMMING_ID"])]

    matching_dimmings_df.to_csv(TEMPORAL_MATCHING_DIMMINGS_DATABASE, index=False)
    matching_dimmings_df.to_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)

if __name__ == "__main__":
    clear_screen()

    find_temporal_matching_dimmings()

    clear_screen()
