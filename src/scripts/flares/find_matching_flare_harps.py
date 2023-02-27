import pandas as pd
from src.cmesrc.config import HARPS_MATCHING_FLARES_DATABASE_PICKLE, SCORED_HARPS_MATCHING_FLARES_DATABASE, SCORED_HARPS_MATCHING_FLARES_DATABASE_PICKLE, MAIN_DATABASE_PICKLE, MAIN_DATABASE
import numpy as np
from tqdm import tqdm

DEG_TO_RAD = np.pi / 180
HALF_POINTS_DIST = 10 * DEG_TO_RAD
NO_POINTS_DIST = 15 * DEG_TO_RAD

print(HALF_POINTS_DIST, NO_POINTS_DIST)

flares_harps_data = pd.read_pickle(HARPS_MATCHING_FLARES_DATABASE_PICKLE)
flares_harps_data.set_index("CME_HARPNUM_FLARE_ID",drop=False, inplace=True)

distances = flares_harps_data["FLARE_HARPS_DIST"].to_numpy()

flares_harps_data["POSITION_SCORES"] = np.piecewise(
        distances,
        [distances <= NO_POINTS_DIST, distances > NO_POINTS_DIST],
        [
            lambda x : 100 * np.exp(- (np.log(2) / HALF_POINTS_DIST ** 2) * x ** 2),
            lambda x : 0
            ]
        )

unmatched_flares = 0
matched_flares = 0

scored_data = flares_harps_data.copy()

for flare_id, group in tqdm(flares_harps_data.groupby("FLARE_ID")):
    filtered_group = group[group["POSITION_SCORES"] > 0].copy()

    no_position_score_indices = group[group["POSITION_SCORES"] == 0].index
    scored_data.loc[no_position_score_indices, "MATCH"] = 0

    if len(filtered_group) == 0:
        unmatched_flares += 1
        continue

    filtered_group.sort_values(by=["FLARE_CME_PA_DIFF", "FLARE_CME_TIME_DIFF", "POSITION_SCORES"], ascending=[True,True,False], inplace=True)

    matching_index = filtered_group.index[0]
    non_matching_indices = filtered_group.index[1:]

    scored_data.loc[matching_index, "MATCH"] = 1
    scored_data.loc[non_matching_indices, "MATCH"] = 0

    matched_flares += 1

print(f"MATCHED FLARES: {matched_flares}")
print(f"UNMATCHED FLARES: {unmatched_flares}")

scored_data.insert(0, 'FLARE_ID', scored_data.pop('FLARE_ID'))
scored_data.sort_values(by=["FLARE_ID", "FLARE_CME_PA_DIFF", "FLARE_CME_TIME_DIFF", "POSITION_SCORES"], ascending=[True, True, True, False], inplace=True)

scored_data.to_csv(SCORED_HARPS_MATCHING_FLARES_DATABASE, index=False)
scored_data.to_pickle(SCORED_HARPS_MATCHING_FLARES_DATABASE_PICKLE)

# And save this to the main dataframe

main_dataframe = pd.read_pickle(MAIN_DATABASE_PICKLE)
main_dataframe.set_index("CME_HARPNUM_ID", drop=False, inplace=True)

for idx, match in scored_data[scored_data["MATCH"] == 1].set_index("CME_HARPNUM_ID", drop=False).iterrows():
    main_dataframe.loc[idx, "FLARE_MATCH"] = match["FLARE_ID"]
    main_dataframe.loc[idx, "FLARE_LON"] = match["FLARE_LON"]
    main_dataframe.loc[idx, "FLARE_LAT"] = match["FLARE_LAT"]

main_dataframe.to_csv(MAIN_DATABASE, index=False)
main_dataframe.to_pickle(MAIN_DATABASE_PICKLE)
