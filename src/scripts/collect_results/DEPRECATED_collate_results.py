from src.cmesrc.config import SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, MAIN_DATABASE, MAIN_DATABASE_PICKLE, DIMMINGS_MATCHED_TO_HARPS_PICKLE, FLARES_MATCHED_TO_HARPS_PICKLE
import pandas as pd
import numpy as np
from astropy.time import Time
import astropy.units as u
from tqdm import tqdm

MAX_DIMMING_TIME_BEFORE_CME = 4 * u.hour
MAX_DIMMING_TIME_AFTER_CME = 0 * u.hour

MAX_FLARE_TIME_BEFORE_CME = 4 * u.hour
MAX_FLARE_TIME_AFTER_CME = 0 * u.hour

# Read in the data

main_database = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)

flares_data = pd.read_pickle(FLARES_MATCHED_TO_HARPS_PICKLE)
dimmings_data = pd.read_pickle(DIMMINGS_MATCHED_TO_HARPS_PICKLE)

main_database.set_index("CME_HARPNUM_ID", drop=False, inplace=True)

# Matched flares and dimmings to HARPs

matched_flares = flares_data[flares_data["MATCH"]]
matched_flares_id_idx = matched_flares.set_index("hec_id", drop=False)
matched_flares.set_index("HARPNUM", drop=True, inplace=True)

matched_dimmings = dimmings_data[dimmings_data["MATCH"]]
matched_dimmings_id_idx = matched_dimmings.set_index("dimming_id", drop=False)
matched_dimmings.set_index("HARPNUM", drop=True, inplace=True)

harps_flares_dict = {
    harps_id: {
        flare_id: False for flare_id in flares_id
    } for harps_id, flares_id in matched_flares.groupby("HARPNUM")["hec_id"]
}

harps_dimmings_dict = {
    harps_id: {
        dimming_id: False for dimming_id in dimmings_id
    } for harps_id, dimmings_id in matched_dimmings.groupby("HARPNUM")["dimming_id"]
}

cmes_dict = {
    cme_id : False for cme_id in main_database["CME_ID"].unique()
}

dimmings_cme_rows = []
flares_cme_rows = []

for harps_id, group in tqdm(main_database.groupby("HARPNUM")):
    cme_dates = Time(group["CME_DATE"].to_numpy())
    cme_ids = group["CME_ID"].to_numpy()

    if harps_id not in matched_dimmings.index and harps_id not in matched_flares.index:
        continue

    if harps_id in matched_dimmings.index:
        dimming_dates = Time(matched_dimmings.loc[harps_id]["max_detection_time"])
        dimming_ids = np.array(matched_dimmings.loc[harps_id]["dimming_id"])

        dimming_cme_diffs = cme_dates[:, np.newaxis] - dimming_dates

        repeated_dimming_ids = np.repeat(dimming_ids, len(cme_ids))

        repeated_cme_ids = np.tile(cme_ids, dimming_ids.size)

        for cme_id, dimming_id, time_diff in zip(repeated_cme_ids, repeated_dimming_ids, dimming_cme_diffs.flatten()):
            dimmings_cme_rows.append(
                {
                    "CME_ID": cme_id,
                    "HARPNUM": harps_id,
                    "DIMMING_ID": dimming_id,
                    "TIME_DIFF": time_diff.to(u.hour).value
                }
            ) 
        
    if harps_id in matched_flares.index:
        flare_dates = Time(matched_flares.loc[harps_id]["time_peak"])
        flare_ids = np.array(matched_flares.loc[harps_id]["hec_id"])

        flare_cme_diffs = cme_dates[:, np.newaxis] - flare_dates

        repeated_flare_ids = np.repeat(flare_ids, len(cme_ids))

        repeated_cme_ids = np.tile(cme_ids, flare_ids.size)

        for cme_id, flare_id, time_diff in zip(repeated_cme_ids, repeated_flare_ids, flare_cme_diffs.flatten()):
            flares_cme_rows.append(
                {
                    "CME_ID": cme_id,
                    "HARPNUM": harps_id,
                    "FLARE_ID": flare_id,
                    "TIME_DIFF": time_diff.to(u.hour).value
                }
            )

dimming_cme_df = pd.DataFrame(dimmings_cme_rows)
flare_cme_df = pd.DataFrame(flares_cme_rows)

possible_dimming_cmes = dimming_cme_df[
    (dimming_cme_df["TIME_DIFF"].between(-MAX_DIMMING_TIME_BEFORE_CME.to(u.hour).value, MAX_DIMMING_TIME_AFTER_CME.to(u.hour).value))
].copy()

possible_flare_cmes = flare_cme_df[
    (flare_cme_df["TIME_DIFF"].between(-MAX_FLARE_TIME_BEFORE_CME.to(u.hour).value, MAX_FLARE_TIME_AFTER_CME.to(u.hour).value))
].copy()

merged = pd.merge(possible_dimming_cmes, possible_flare_cmes, on=["CME_ID", "HARPNUM"])

merged["DIMMING_FLARE_DIFF"] = np.abs(merged["TIME_DIFF_x"] - merged["TIME_DIFF_y"])

mask = merged["DIMMING_FLARE_DIFF"] < 1

# Now we can first assign the ones where a priori we can bundle together
# the dimming and the flare

merged_accepted = merged[mask].sort_values("DIMMING_FLARE_DIFF").drop_duplicates(subset=["CME_ID", "HARPNUM"],
keep="first").drop_duplicates(subset=["DIMMING_ID"],
keep="first").drop_duplicates(subset=["FLARE_ID"])

for idx, row in merged_accepted.iterrows():
    cme_id = row["CME_ID"]
    dimming_id = row["DIMMING_ID"]
    flare_id = row["FLARE_ID"]
    harpnum = row["HARPNUM"]

    if cmes_dict[cme_id]:
        raise ValueError("CME ID already matched")

    if harps_dimmings_dict[harpnum][dimming_id]:
        raise ValueError("Dimming ID already matched")

    if harps_flares_dict[harpnum][flare_id]:
        raise ValueError("Flare ID already matched")

    cmes_dict[row["CME_ID"]] = True
    harps_dimmings_dict[row["HARPNUM"]][row["DIMMING_ID"]] = True
    harps_flares_dict[row["HARPNUM"]][row["FLARE_ID"]] = True

    dimming_row = matched_dimmings_id_idx.loc[dimming_id]
    flare_row = matched_flares_id_idx.loc[flare_id]

    # Just to keep track of numbers

    matched_dimmings_id_idx.at[dimming_id, "CME_MATCH"] = True
    matched_flares_id_idx.at[flare_id, "CME_MATCH"] = True

    main_id = f"{cme_id}{harpnum}"

    main_database.at[main_id, "DIMMING_MATCH"] = dimming_id
    main_database.at[main_id, "DIMMING_LON"] = dimming_row["longitude"]
    main_database.at[main_id, "DIMMING_LAT"] = dimming_row["latitude"]
    main_database.at[main_id, "DIMMING_FLAG"] = True

    main_database.at[main_id, "FLARE_MATCH"] = flare_id
    main_database.at[main_id, "FLARE_LON"] = flare_row["long_hg"]
    main_database.at[main_id, "FLARE_LAT"] = flare_row["lat_hg"]
    main_database.at[main_id, "FLARE_FLAG"] = True
    main_database.at[main_id, "FLARE_CLASS"] = flare_row["xray_class"]
    main_database.at[main_id, "FLARE_CLASS_SCORE"] = flare_row["FLARE_CLASS"]
    main_database.at[main_id, "FLARE_CLASS_FLAG"] = flare_row["FLARE_CLASS"] > 25


# The rest of dimmings and flares are just matched by how close they're to a CME
# The closest is chosen

possible_dimming_cmes["ABS_TIME_DIFF"] = np.abs(possible_dimming_cmes["TIME_DIFF"])

dimming_alone_accepted = possible_dimming_cmes.sort_values(by="ABS_TIME_DIFF",
ascending=True).drop_duplicates(subset=["CME_ID"],
keep="first").drop_duplicates(subset=["DIMMING_ID"], keep="first")

for idx, row in dimming_alone_accepted.iterrows():
    cme_id = row["CME_ID"]
    dimming_id = row["DIMMING_ID"]
    harpnum = row["HARPNUM"]

    if cmes_dict[cme_id]:
        continue

    if harps_dimmings_dict[harpnum][dimming_id]:
        continue

    cmes_dict[row["CME_ID"]] = True
    harps_dimmings_dict[row["HARPNUM"]][row["DIMMING_ID"]] = True

    matched_dimmings_id_idx.at[dimming_id, "CME_MATCH"] = True

    main_id = f"{cme_id}{harpnum}"

    main_database.at[main_id, "DIMMING_MATCH"] = dimming_id
    main_database.at[main_id, "DIMMING_LON"] = dimming_row["longitude"]
    main_database.at[main_id, "DIMMING_LAT"] = dimming_row["latitude"]
    main_database.at[main_id, "DIMMING_FLAG"] = True


possible_flare_cmes["ABS_TIME_DIFF"] = np.abs(possible_flare_cmes["TIME_DIFF"])

flare_alone_accepted = possible_flare_cmes.sort_values(by="ABS_TIME_DIFF",
ascending=True).drop_duplicates(subset=["CME_ID"],
keep="first").drop_duplicates(subset=["FLARE_ID"], keep="first")


for idx, row in flare_alone_accepted.iterrows():
    cme_id = row["CME_ID"]
    flare_id = row["FLARE_ID"]
    harpnum = row["HARPNUM"]

    if cmes_dict[cme_id]:
        continue

    if harps_flares_dict[harpnum][flare_id]:
        continue

    cmes_dict[row["CME_ID"]] = True
    harps_flares_dict[row["HARPNUM"]][row["FLARE_ID"]] = True

    matched_flares_id_idx.at[flare_id, "CME_MATCH"] = True

    main_id = f"{cme_id}{harpnum}"

    main_database.at[main_id, "FLARE_MATCH"] = flare_id
    main_database.at[main_id, "FLARE_LON"] = flare_row["long_hg"]
    main_database.at[main_id, "FLARE_LAT"] = flare_row["lat_hg"]
    main_database.at[main_id, "FLARE_FLAG"] = True
    main_database.at[main_id, "FLARE_CLASS"] = flare_row["xray_class"]
    main_database.at[main_id, "FLARE_CLASS_SCORE"] = flare_row["FLARE_CLASS"]
    main_database.at[main_id, "FLARE_CLASS_FLAG"] = flare_row["FLARE_CLASS"] > 25

main_database.to_csv(MAIN_DATABASE, index=False)
main_database.to_pickle(MAIN_DATABASE_PICKLE)

