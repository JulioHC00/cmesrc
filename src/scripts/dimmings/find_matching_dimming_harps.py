import pandas as pd
from src.cmesrc.config import HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE, SCORED_HARPS_MATCHING_DIMMINGS_DATABASE, SCORED_HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE, MAIN_DATABASE_PICKLE, MAIN_DATABASE
import numpy as np
from tqdm import tqdm

DEG_TO_RAD = np.pi / 180
HALF_POINTS_DIST = 10 * DEG_TO_RAD
NO_POINTS_DIST = 15 * DEG_TO_RAD

print(HALF_POINTS_DIST, NO_POINTS_DIST)

dimmings_harps_data = pd.read_pickle(HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)
dimmings_harps_data.set_index("CME_HARPNUM_DIMMING_ID",drop=False, inplace=True)

distances = dimmings_harps_data["DIMMING_HARPS_DIST"].to_numpy()

dimmings_harps_data["POSITION_SCORES"] = np.piecewise(
        distances,
        [distances <= NO_POINTS_DIST, distances > NO_POINTS_DIST],
        [
            lambda x : 100 * np.exp(- (np.log(2) / HALF_POINTS_DIST ** 2) * x ** 2),
            lambda x : 0
            ]
        )

unmatched_dimmings = 0
matched_dimmings = 0

scored_data = dimmings_harps_data.copy()

for dimming_id, group in tqdm(dimmings_harps_data.groupby("DIMMING_ID")):
    filtered_group = group[group["POSITION_SCORES"] > 0].copy()

    if len(filtered_group) == 0:
        unmatched_dimmings += 1
        continue

    filtered_group.sort_values(by=["DIMMING_CME_PA_DIFF", "DIMMING_CME_TIME_DIFF", "POSITION_SCORES"], ascending=[True,True,False], inplace=True)

#   print(filtered_group[["DIMMING_CME_PA_DIFF", "DIMMING_CME_TIME_DIFF", "POSITION_SCORES"]])
    matching_index = filtered_group.index[0]
    non_matching_indices = filtered_group.index[1:]

    scored_data.loc[matching_index, "MATCH"] = 1
    scored_data.loc[non_matching_indices, "MATCH"] = 0

    matched_dimmings += 1

print(f"MATCHED DIMMINGS: {matched_dimmings}")
print(f"UNMATCHED DIMMINGS: {unmatched_dimmings}")

scored_data.insert(0, 'DIMMING_ID', scored_data.pop('DIMMING_ID'))
scored_data.sort_values(by=["DIMMING_ID", "DIMMING_CME_PA_DIFF", "DIMMING_CME_TIME_DIFF", "POSITION_SCORES"], ascending=[True, True, True, False], inplace=True)

scored_data.to_csv(SCORED_HARPS_MATCHING_DIMMINGS_DATABASE, index=False)
scored_data.to_pickle(SCORED_HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)

# And save this to the main dataframe

main_dataframe = pd.read_pickle(MAIN_DATABASE_PICKLE)
main_dataframe.set_index("CME_HARPNUM_ID", drop=False, inplace=True)

for idx, match in scored_data[scored_data["MATCH"] == 1].set_index("CME_HARPNUM_ID", drop=False).iterrows():
    main_dataframe.loc[idx, "DIMMING_MATCH"] = match["DIMMING_ID"]
    main_dataframe.loc[idx, "DIMMING_LON"] = match["DIMMING_LON"]
    main_dataframe.loc[idx, "DIMMING_LAT"] = match["DIMMING_LAT"]

main_dataframe.to_csv(MAIN_DATABASE, index=False)
main_dataframe.to_pickle(MAIN_DATABASE_PICKLE)
