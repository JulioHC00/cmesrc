import pandas as pd
from src.cmesrc.config import HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE, SCORED_HARPS_MATCHING_DIMMINGS_DATABASE, SCORED_HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE, MAIN_DATABASE_PICKLE, MAIN_DATABASE
from src.cmesrc.utils import clear_screen
import numpy as np
from tqdm import tqdm

DEG_TO_RAD = np.pi / 180
HALF_POINTS_DIST = 10 * DEG_TO_RAD
NO_POINTS_DIST = 15 * DEG_TO_RAD

def score_dimmings():
    print("===DIMMINGS===")
    print("==Scoring and matching dimmings==")
    dimmings_harps_data = pd.read_pickle(HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE)
    dimmings_harps_data.set_index("CME_HARPNUM_DIMMING_ID",drop=False, inplace=True)

    distances = dimmings_harps_data["DIMMING_HARPS_DIST"].to_numpy()

    # I think that this function that I chose is overkill, any function that is
    # decreasing with increasing distance should work but whatever
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
        
        # The problem here is that a dimming may be close to different CMEs in
        # time and in PA so if that's the case, how do we choose to which one
        # does it belong? Hopefully, since by definition if this happens the
        # time between the CMEs is small, the positions of the HARPS shouldn't
        # have changed much so the position scores should be the same and we
        # match the dimming to the same region no matter what CME we choose.
        # 
        # But even if we choose the wrong CME, it's probably fine too. Keep in
        # mind we're only trying to see if the region produced a CME or not and
        # if we match it to a CME that happened say 2 hours after the real one
        # that shouldn't affect the ML training but it will affect how you use
        # the catalogue for other things
        # 
        # So for now what I do is first sort by PA difference, because the
        # closest the dimming to where the CME is seen in LASCO the more likely
        # it's to be the true one. Because what may happen is that we have two
        # CMEs A and B which happend one after the other and then we have two
        # regions X and Y. X produces A and Y produces B. Now, say A and B
        # happened very close in time so we have matched the dimming temporally
        # to both of them and now we have them both in the filtered group here.
        # Say the dimming belongs to Y.  Now, if we sort by time difference only
        # and the dimming happened before A we'll assing the dimming to CME A
        # and correctly to region Y but because region Y is not in the right 
        # position for whete A was seen, we don't match it anything and both
        # CMEs are left unmatched. Now, if we first sort by PA difference then B
        # will come first as the dimming is close to where B was seen, we match
        # it to Y and we get a match for the CME B.
        # 
        # Why we need to sort by time difference too is in the unlikely scenario
        # that the two CMEs have the exact same PA, but in that case they
        # probably come from the same region so if we mismatch by a few hours it
        # should be fine

        # NOTE: This doesn't work with halo CMEs, need to solve it

        # Proposed approach, instead of using the PA difference, if a region appears twice,
        # keep only the one that has spatial consistency so in that case we ensure that
        # we give priority to the cme that happened where the region was.

        # I think this should remove the need for sorting by DIMMING_CME_PA_DIFF
        # And since it's not clear that being closer in time to the CME is indivcative of an
        # association, I won't sort by time difference

        # CONCLUSION
        # As of now, how it is working is simply by sorting by how close the dimming is to any
        # of the regions and to solve ambiguities between different cmes, we can solve these
        # if the regions were spatially consistent in one cme and not in another
        #
        # In principle, this should be good for our purposes since what we care is what
        # source region did actually produce the CME, so if we missmatch by a few hours
        # one that produced a cme in both of those or that didn't produce it in any
        # it should be fine

        # First we have to group by HARPNUM

        harps_filtered_groups = filtered_group.groupby("HARPNUM")

        # Now we loop to see if there are any duplicates

        for harpnum, harps_filtered_group in harps_filtered_groups:
            if len(harps_filtered_group) > 1:
                # There are duplicates
                # See if there are spatially consistent ones

                if np.any(harps_filtered_group["HARPS_SPAT_CONSIST"]):
                    # There are spatially consistent ones, keep only those
                    filtered_group.drop(harps_filtered_group[~harps_filtered_group["HARPS_SPAT_CONSIST"]].index, inplace=True)


        filtered_group.sort_values(by=["POSITION_SCORES"], ascending=[False], inplace=True)

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

    # First, if the columns already exist, we need to reset them
    if "DIMMING_MATCH" in main_dataframe.columns:
        main_dataframe["DIMMING_MATCH"] = np.nan
    if "DIMMING_LON" in main_dataframe.columns:
        main_dataframe["DIMMING_LON"] = np.nan
    if "DIMMING_LAT" in main_dataframe.columns:
        main_dataframe["DIMMING_LAT"] = np.nan
    if "DIMMING_FLAG" in main_dataframe.columns:
        main_dataframe["DIMMING_FLAG"] = np.nan
    

    for idx, match in scored_data[scored_data["MATCH"] == 1].set_index("CME_HARPNUM_ID", drop=False).iterrows():
        main_dataframe.loc[idx, "DIMMING_MATCH"] = match["DIMMING_ID"]
        main_dataframe.loc[idx, "DIMMING_LON"] = match["DIMMING_LON"]
        main_dataframe.loc[idx, "DIMMING_LAT"] = match["DIMMING_LAT"]
        main_dataframe.loc[idx, "DIMMING_FLAG"] = True

    main_dataframe["DIMMING_FLAG"] = main_dataframe["DIMMING_FLAG"].fillna(False)

    main_dataframe.to_csv(MAIN_DATABASE, index=False)
    main_dataframe.to_pickle(MAIN_DATABASE_PICKLE)

if __name__ == "__main__":
    clear_screen()

    score_dimmings()

    clear_screen()
