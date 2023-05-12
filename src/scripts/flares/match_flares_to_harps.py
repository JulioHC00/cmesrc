import pandas as pd
from astropy.time import Time
from tqdm import tqdm
from src.harps.harps import Harps
from src.flares.flares import Flare
from src.cmesrc.utils import clear_screen, filepaths_updated_swan_data, get_closest_harps_timestamp, read_SWAN_filepath
import numpy as np
import json
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

SWAN = filepaths_updated_swan_data()

#SWAN = {
#    "test": pd.read_csv("/home/julio/cmesrc/data/interim/SWAN/345.csv", sep="\t")
#}

flares_rows = []

print("== EXTRACTING FLARES FROM SWAN-SF ==")

for harpnum, harp_filepath in tqdm(SWAN.items()):
    harp_data = read_SWAN_filepath(harp_filepath)
    flare_mask = harp_data[["BFLARE","CFLARE","MFLARE","XFLARE"]].any(axis=1)   

    for idx, row in harp_data[flare_mask].iterrows():

        harp_data = row[["Timestamp", "LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"]].to_list()

        harps = Harps(*harp_data)

        for fclass in ["B", "C", "M", "X"]:
            if row[f"{fclass}FLARE"]:
                all_flares_data = row[f"{fclass}FLARE_LABEL"].split(";")

                for flare_data in all_flares_data:
                    flare_data = json.loads(flare_data)

                    new_flare = {
                        "HARPNUM": harpnum,
                        "FLARE_ID": flare_data["id"],
                        "FLARE_DATE": row["Timestamp"],
                        "FLARE_LON": harps.get_centre_point().LON,
                        "FLARE_LAT": harps.get_centre_point().LAT,
                        "FLARE_CLASS_SCORE": flare_class_to_number(flare_data["magnitude"]),
                        "FLARE_CLASS": flare_data["magnitude"],
                        "FLARE_AR": flare_data["NOAA_AR"],
                        "FLARE_AR_SOURCE": flare_data["narn_source"],
                        "FLARE_VERIFICATION": flare_data["verification"],
                    }

                    flares_rows.append(new_flare)
    
flares_data = pd.DataFrame(flares_rows)

# Test for duplicates before saving

duplicate_matches = flares_data.duplicated(subset=["FLARE_ID"], keep=False)

if np.any(duplicate_matches):
    print(flares_data[duplicate_matches].sort_values("FLARE_ID"))
    raise ValueError("Duplicate matches found")

flares_data.to_csv(FLARES_MATCHED_TO_HARPS, index=False)
flares_data.to_pickle(FLARES_MATCHED_TO_HARPS_PICKLE)