from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, DIMMINGS_MATCHED_TO_HARPS_PICKLE, FLARES_MATCHED_TO_HARPS_PICKLE, MAIN_DATABASE, MAIN_DATABASE_PICKLE
import pandas as pd
import numpy as np
from astropy.time import Time
import astropy.units as u
from datetime import timedelta
from tqdm import tqdm

MAX_DIMMING_TIME_BEFORE_CME = 3 * u.hour
MAX_DIMMING_TIME_AFTER_CME = 0 * u.hour

MAX_FLARE_TIME_BEFORE_CME = 3 * u.hour
MAX_FLARE_TIME_AFTER_CME = 0 * u.hour

MIN_FLARE_CLASS = 25

# Read in the data

def collate_results():
    main_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
    flares_data = pd.read_pickle(FLARES_MATCHED_TO_HARPS_PICKLE)
    dimmings_data = pd.read_pickle(DIMMINGS_MATCHED_TO_HARPS_PICKLE)

    main_database.set_index("CME_HARPNUM_ID", drop=False, inplace=True)

    main_database.sort_values("HARPS_SPAT_CONSIST", inplace=True, ascending=False)

    # dimmings_data["start_time"] = Time(dimmings_data["max_detection_time"])
    # flares_data["FLARE_DATE"] = Time(flares_data["time_peak"])

    flares_data = flares_data[flares_data["FLARE_VERIFICATION"].apply(lambda x: x != "Non-verified")]
    dimmings_data = dimmings_data[dimmings_data["MATCH"]]

    dimmings_data["start_time"] = Time(dimmings_data["start_time"].to_list())
    flares_data["FLARE_DATE"] = Time(flares_data["FLARE_DATE"].to_list())

    grouped_main_database = main_database.copy().groupby("HARPNUM")
    grouped_flares_data = flares_data.groupby("HARPNUM")
    grouped_dimmings_data = dimmings_data.groupby("HARPNUM")

    matched_flares = []
    matched_dimmings = []

    harpnum_set = set(main_database["HARPNUM"])

    for harpnum, cmes in tqdm(grouped_main_database, total=len(harpnum_set)):
        for main_idx, cme in cmes.iterrows():
            cme_date = cme["CME_DATE"]

            if harpnum in grouped_flares_data.groups:
                flares = grouped_flares_data.get_group(harpnum)

                possible_flares = flares[
                    (flares["FLARE_DATE"] >= cme_date - MAX_FLARE_TIME_BEFORE_CME) &
                    (flares["FLARE_DATE"] <= cme_date + MAX_FLARE_TIME_AFTER_CME) &
                    (~ flares["FLARE_ID"].isin(matched_flares))
                ].copy()

                if len(possible_flares) > 0:
                    # Sort by class
                    possible_flares = possible_flares.sort_values("FLARE_CLASS", ascending=False)

                    # Get the first one
                    flare = possible_flares.iloc[0]

                    # Add to the matched flares

                    matched_flares.append(flare["FLARE_ID"])

                    # Add to the main database

                    main_database.loc[main_idx, "FLARE_CLASS"] = flare["FLARE_CLASS"]
                    main_database.loc[main_idx, "FLARE_FLAG"] = True
                    main_database.loc[main_idx, "FLARE_MATCH"] = flare["FLARE_ID"]
                    main_database.loc[main_idx, "FLARE_CLASS_FLAG"] = flare["FLARE_CLASS_SCORE"] >= MIN_FLARE_CLASS

            if harpnum in grouped_dimmings_data.groups:
                dimmings = grouped_dimmings_data.get_group(harpnum)

                possible_dimmings = dimmings[
                    (dimmings["start_time"] >= cme_date - MAX_DIMMING_TIME_BEFORE_CME) &
                    (dimmings["start_time"] <= cme_date + MAX_DIMMING_TIME_AFTER_CME) &
                    (~ dimmings["dimming_id"].isin(matched_dimmings))
                ].copy()

                if len(possible_dimmings) > 0:
                    # Sort by closest to CME

                    possible_dimmings["time_diff"] = [dimming_time - cme_date for dimming_time in possible_dimmings["start_time"]]


                    possible_dimmings = possible_dimmings.sort_values("time_diff", ascending=True)

                    # Get the first one
                    dimming = possible_dimmings.iloc[0]

                    # Add to the matched dimmings

                    matched_dimmings.append(dimming["dimming_id"])

                    # Add to the main database

                    main_database.loc[main_idx, "DIMMING_MATCH"] = dimming["dimming_id"]
                    main_database.loc[main_idx, "DIMMING_FLAG"] = True
    
    # Test no duplicates

    dimming_duplicates = main_database.duplicated(subset="DIMMING_MATCH", keep=False)
    flare_duplicates = main_database.duplicated(subset="FLARE_MATCH", keep=False)

    if dimming_duplicates.any():
        print("Duplicates in dimming matches")
        print(main_database[dimming_duplicates])
    if flare_duplicates.any():
        print("Duplicates in flare matches")
        print(main_database[flare_duplicates])
    if dimming_duplicates.any() or flare_duplicates.any():
        raise ValueError("Duplicates in matches")

    main_database.to_pickle(MAIN_DATABASE_PICKLE)
    main_database.to_csv(MAIN_DATABASE, index=False)

if __name__ == "__main__":
    collate_results()