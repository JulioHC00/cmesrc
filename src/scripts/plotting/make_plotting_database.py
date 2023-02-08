import pandas as pd
from src.cmesrc.config import TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE, HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE, PLOTTING_DATABASE, PLOTTING_DATABASE_PICKLE
print(PLOTTING_DATABASE)

cme_harps_temporal = pd.read_pickle(TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
cme_harps_temporal.set_index("id", drop=False, inplace=True)
cme_harps_spatiotemportal = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
cme_harps_spatiotemportal["cme_id"] = [int(item) for item in cme_harps_spatiotemportal["cme_id"].to_list()]
group_cme_harps_spatiotemporal = cme_harps_spatiotemportal.groupby("cme_id")

cme_dimmings_temporal = pd.read_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)
cme_dimmings_temporal["cme_id"] = [int(item) for item in cme_dimmings_temporal["cme_id"].to_list()] 
cme_dimmings_temporal.set_index("cme_id", drop=False, inplace=True)
cme_dimmings_spatiotemporal = pd.read_pickle(HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)
cme_dimmings_spatiotemporal = cme_dimmings_spatiotemporal[cme_dimmings_spatiotemporal["match"]]
cme_dimmings_spatiotemporal["cme_id"] = [int(item) for item in cme_dimmings_spatiotemporal["cme_id"].to_list()] 
grouped_cme_dimmings_spatiotemporal = cme_dimmings_spatiotemporal.groupby("cme_id")

data = []

for cme_id, temporal_harps_data in cme_harps_temporal.iterrows():
    new_row = dict()

    new_row["cme_id"] = cme_id
    new_row["cme_date"] = temporal_harps_data["date"]
    new_row["cme_pa"] = temporal_harps_data["pa"]
    new_row["cme_width"] = temporal_harps_data["width"]
    new_row["cme_halo"] = bool(temporal_harps_data["halo"])

    all_match_harps = set(temporal_harps_data["matching_harps"])

    new_row["all_harps"] = all_match_harps

    if cme_id in group_cme_harps_spatiotemporal.groups:
        spatiotemporal_harps = set(group_cme_harps_spatiotemporal.get_group(cme_id)["harpnum"].to_list())
    else:
        spatiotemporal_harps = set()

    non_spatial_harps = all_match_harps - spatiotemporal_harps

    new_row["non_spatial_harps"] = non_spatial_harps
    new_row["spatial_harps"] = spatiotemporal_harps

    if cme_id in cme_dimmings_temporal.index:
        all_dimmings = set(cme_dimmings_temporal.loc[cme_id]["dimming_ids"])
    else:
        all_dimmings = set()

    new_row["all_dimmings"] = all_dimmings

    if cme_id in grouped_cme_dimmings_spatiotemporal.groups:
        spatiotemporal_dimmings = {
                row["dimming_id"] : row["harpnum"] for idx, row in grouped_cme_dimmings_spatiotemporal.get_group(cme_id).iterrows()
                }
    else:
        spatiotemporal_dimmings = dict()

    non_spatial_dimmings = all_dimmings - set(spatiotemporal_dimmings.keys())

    new_row["non_spatial_dimmings"] = non_spatial_dimmings
    new_row["spatial_dimmings"] = spatiotemporal_dimmings

    data.append(new_row)

df = pd.DataFrame.from_records(data)

df.to_csv(PLOTTING_DATABASE, index=False)
df.to_pickle(PLOTTING_DATABASE_PICKLE)
