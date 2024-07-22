import sys
from tqdm import tqdm
import bisect
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import subprocess
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from src.cmesrc.config import (
    CMESRC_DB,
    CMESRC_BBOXES,
    HARPNUM_TO_NOAA,
    LASCO_CME_DATABASE,
    SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE,
    DIMMINGS_MATCHED_TO_HARPS_PICKLE,
    FLARES_MATCHED_TO_HARPS_PICKLE,
)
from src.cmesrc.utils import read_SWAN_filepath, filepaths_updated_swan_data


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


# Copy CMESRC_BBOXES to CMESRC_DB
os.system(f"cp {CMESRC_BBOXES} {CMESRC_DB}")

# Create CMESRC_BBOXES
new_conn = sqlite3.connect(CMESRC_DB)
new_cur = new_conn.cursor()

clear_screen()

print("Loading scripts data into database")
# Now load LASCO CME catalogue

df = pd.read_csv(LASCO_CME_DATABASE)

df[df.duplicated(subset="CME_ID", keep=False)]

no_duplicates = df.drop_duplicates(subset="CME_ID", keep=False)

for i, row in no_duplicates.drop_duplicates(subset=["CME_ID"], keep="first").iterrows():
    new_conn.execute(
        """
        INSERT INTO CMES (cme_id, cme_date, cme_pa, cme_width, cme_halo, cme_seen_in, cme_quality, cme_three_points)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(row["CME_ID"][2:]),
            row["CME_DATE"],
            row["CME_PA"],
            row["CME_WIDTH"],
            row["CME_HALO"],
            row["CME_SEEN_IN"],
            row["CME_QUALITY"],
            row["CME_THREE_POINTS"],
        ),
    )

# Spatially consistent CME-HARP associations

df = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
df = df[df["HARPS_SPAT_CONSIST"]]

for cme_id, harpnum in df[["CME_ID", "HARPNUM"]].values:
    cme_id = int(cme_id[2:])
    harpnum = int(harpnum)

    # Add the match to CMES_HARPS_SPATIALLY_CONSIST

    new_cur.execute(
        """
                INSERT INTO CMES_HARPS_SPATIALLY_CONSIST (harpnum, cme_id)
                VALUES (?, ?)
                """,
        (harpnum, cme_id),
    )

new_conn.commit()

# Now dimmings

dimmings = pd.read_pickle(DIMMINGS_MATCHED_TO_HARPS_PICKLE)
dimmings = dimmings[dimmings["MATCH"]]

for i, row in dimmings[dimmings["MATCH"]].iterrows():
    new_cur.execute(
        """
        INSERT INTO dimmings (dimming_id, harpnum, harps_dimming_dist, dimming_start_date, dimming_peak_date, dimming_lon, dimming_lat)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(row["dimming_id"]),
            int(row["HARPNUM"]),
            row["HARPS_DIMMING_DISTANCE"],
            row["start_time"].split(".")[0],
            row["max_detection_time"].iso[:-4],
            row["longitude"],
            row["latitude"],
        ),
    )

new_conn.commit()

# Now flares
flares = pd.read_pickle(FLARES_MATCHED_TO_HARPS_PICKLE)

for i, row in flares.iterrows():
    new_cur.execute(
        """
        INSERT INTO flares (flare_id, HARPNUM, flare_date, flare_lon, flare_lat, flare_class_score, flare_class, flare_ar, flare_ar_source, flare_verification)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(row["FLARE_ID"]),
            int(row["HARPNUM"]),
            row["FLARE_DATE"].iso[:-4],
            row["FLARE_LON"],
            row["FLARE_LAT"],
            row["FLARE_CLASS_SCORE"],
            row["FLARE_CLASS"],
            row["FLARE_AR"],
            row["FLARE_AR_SOURCE"],
            row["FLARE_VERIFICATION"],
        ),
    )

new_conn.commit()

# Now calculate their closest processed_harps_bbox timestamps
# Note it says "image_timestamps" because originally I was doing it
# with SDOML images but it's more appropriate to do it with
# SHARP timestamps
# So in the database they are still referred to as image_timestamp but they're
# SHARPs timestamps

print("Calculating closest timestamps for dimmings, flares and CMEs...")


def closest_timestamp(target, sorted_timestamps):
    index = bisect.bisect_left(sorted_timestamps, target)
    if index == 0:
        return sorted_timestamps[0]
    if index == len(sorted_timestamps):
        return sorted_timestamps[-1]
    before = sorted_timestamps[index - 1]
    after = sorted_timestamps[index]
    if after - target < target - before:
        return after
    else:
        return before


def formatted_timestamp(timestamp):
    """
    Format it to be the same hour and the closest minutes out
    of 00, 12, 24, 36, 48

    Using datetime
    """
    minutes = [0, 12, 24, 36, 48]

    # Check if already is a datetime
    if not isinstance(timestamp, datetime):
        datetime_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    else:
        datetime_timestamp = timestamp

    # The candidate timestamps will be either same timestamp but
    # with 00 minutes, 12 minutes, 36 minutes, 48 minutes or the next hour with 00 minutes
    # All with 0 seconds

    candidate_timestamps = [
        datetime_timestamp.replace(minute=minute).replace(second=0)
        for minute in minutes
    ]

    candidate_timestamps.append(
        datetime_timestamp.replace(minute=0).replace(second=0) + timedelta(hours=1)
    )

    # Sort them

    candidate_timestamps = sorted(candidate_timestamps)

    # Now we use bisect to find the closest minute

    new_timestamp = closest_timestamp(datetime_timestamp, candidate_timestamps)

    return new_timestamp


# Dimmings
for row in tqdm(
    new_cur.execute("SELECT dimming_id, dimming_start_date FROM dimmings").fetchall()
):
    dimming_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(dimming_timestamp)
    new_cur.execute(
        "UPDATE dimmings SET image_timestamp = ? WHERE dimming_id = ?",
        (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]),
    )

# Same for flares
for row in tqdm(new_cur.execute("SELECT flare_id, flare_date FROM flares").fetchall()):
    flare_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(flare_timestamp)
    new_cur.execute(
        "UPDATE flares SET image_timestamp = ? WHERE flare_id = ?",
        (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]),
    )

# And for cmes
for row in tqdm(new_cur.execute("SELECT cme_id, cme_date FROM cmes").fetchall()):
    cme_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(cme_timestamp)
    new_cur.execute(
        "UPDATE cmes SET image_timestamp = ? WHERE cme_id = ?",
        (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]),
    )

new_conn.commit()

# Now do the associations
clear_screen()

print("Associating dimmings, flares and CMEs")

association_threshold = 2.01
min_time_before = 12 / 60

results = {}

harps = new_cur.execute("SELECT DISTINCT harpnum from PROCESSED_HARPS_BBOX").fetchall()

new_cur.execute("DELETE FROM CMES_HARPS_EVENTS")

for harp in tqdm(harps):
    harp = harp[0]

    flare_data = new_cur.execute(
        "SELECT flare_date, flare_id, flare_class_score FROM FLARES WHERE harpnum = ? AND flare_verification != 'Non-verified'",
        (harp,),
    ).fetchall()
    flare_timestamps, flare_ids, flare_class_scores = (
        zip(*flare_data) if flare_data else ([], [], [])
    )

    dimming_data = new_cur.execute(
        "SELECT dimming_start_date, dimming_id FROM DIMMINGS WHERE harpnum = ?", (harp,)
    ).fetchall()
    dimming_timestamps, dimming_ids = zip(*dimming_data) if dimming_data else ([], [])

    present_at_cme_data = new_cur.execute(
        """
        SELECT c.cme_date, c.cme_id from CMES_HARPS_SPATIALLY_CONSIST as sch
        INNER JOIN CMES as c
        ON sch.cme_id = c.cme_id
        WHERE sch.harpnum = ? 
    """,
        (harp,),
    ).fetchall()
    present_at_cme_timestamps, present_at_cme_ids = (
        zip(*present_at_cme_data) if present_at_cme_data else ([], [])
    )

    # Convert to datetime objects
    flare_timestamps = [
        datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in flare_timestamps
    ]
    dimming_timestamps = [
        datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in dimming_timestamps
    ]
    present_at_cme_timestamps = [
        datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in present_at_cme_timestamps
    ]

    used_flare_ids = set()
    used_dimming_ids = set()

    for cme_timestamp, cme_id in zip(present_at_cme_timestamps, present_at_cme_ids):
        # For Flares
        closest_flare_id = None
        closest_flare_hours_diff = -1
        if flare_timestamps:
            flare_index = bisect.bisect_right(flare_timestamps, cme_timestamp)
            matching_flares = []

            while flare_index > 0:
                flare_index -= 1
                flare_timestamp = flare_timestamps[flare_index]
                hour_diff = (cme_timestamp - flare_timestamp).total_seconds() / 3600
                if min_time_before < hour_diff < association_threshold:
                    if flare_ids[flare_index] not in used_flare_ids:
                        used_flare_ids.add(flare_ids[flare_index])
                        matching_flares.append(
                            (
                                hour_diff,
                                flare_ids[flare_index],
                                flare_class_scores[flare_index],
                            )
                        )
                elif hour_diff > association_threshold:
                    break

            if matching_flares:
                closest_flare = sorted(matching_flares, key=lambda x: (-x[2], x[0]))[0]
                closest_flare_hours_diff, closest_flare_id, _ = closest_flare

        # For Dimmings
        closest_dimming_id = None
        closest_dimming_hours_diff = -1
        if dimming_timestamps:
            dimming_index = bisect.bisect_right(dimming_timestamps, cme_timestamp)
            matching_dimmings = []

            while dimming_index > 0:
                dimming_index -= 1
                dimming_timestamp = dimming_timestamps[dimming_index]
                hour_diff = (cme_timestamp - dimming_timestamp).total_seconds() / 3600
                if min_time_before < hour_diff < association_threshold:
                    if dimming_ids[dimming_index] not in used_dimming_ids:
                        used_dimming_ids.add(dimming_ids[dimming_index])
                        matching_dimmings.append(
                            (hour_diff, dimming_ids[dimming_index])
                        )
                elif hour_diff > association_threshold:
                    break

            if matching_dimmings:
                closest_dimming = min(matching_dimmings, key=lambda x: x[0])
                closest_dimming_hours_diff, closest_dimming_id = closest_dimming

        results[(harp, cme_id)] = {
            "closest_flare_id": closest_flare_id,
            "closest_flare_hours_diff": closest_flare_hours_diff,
            "closest_dimming_id": closest_dimming_id,
            "closest_dimming_hours_diff": closest_dimming_hours_diff,
        }

for (harp, cme_id), event in tqdm(results.items()):
    new_cur.execute(
        """
        INSERT INTO CMES_HARPS_EVENTS (harpnum, cme_id, flare_id, flare_hours_diff, dimming_id, dimming_hours_diff)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            harp,
            cme_id,
            event["closest_flare_id"],
            event["closest_flare_hours_diff"],
            event["closest_dimming_id"],
            event["closest_dimming_hours_diff"],
        ),
    )

new_conn.commit()

# Now in order to find every match we can choose all rows for each CME and sort
# first by which have a dimming and then by flare class

# Let's see how many potential matches there are

unique_cmes = new_cur.execute(
    "SELECT DISTINCT cme_id from CMES_HARPS_SPATIALLY_CONSIST"
).fetchall()

# Now let's get the matches


def get_verfification_level(has_dimming, has_flare, flare_class, flare_threshold=25):
    if has_dimming:
        if has_flare:
            if flare_class > flare_threshold:
                return 1
            else:
                return 3
        else:
            return 5
    else:
        if has_flare:
            if flare_class > flare_threshold:
                return 2
            else:
                return 4
        else:
            return -1


def verif_level_from_row(row):
    has_dimming = row["has_dimming"]
    has_flare = 0 if pd.isnull(row["flare_id"]) else 1
    flare_class = (
        None if pd.isnull(row["flare_class_score"]) else row["flare_class_score"]
    )
    harpnum = row["harpnum"]

    return get_verfification_level(has_dimming, has_flare, flare_class)


matches = dict()

for unique_cme in tqdm(unique_cmes):
    query = f"""
    SELECT CHSC.cme_id, CHSC.harpnum, CHE.flare_id, CHE.dimming_id, F.flare_class_score from CMES_HARPS_SPATIALLY_CONSIST CHSC
    LEFT JOIN CMES_HARPS_EVENTS CHE ON CHSC.cme_id = CHE.cme_id AND CHSC.harpnum = CHE.harpnum
    LEFT JOIN FLARES F ON CHE.flare_id = F.flare_id
    WHERE CHSC.cme_id = {unique_cme[0]}
    """

    df = pd.read_sql_query(query, new_conn)

    # Need to replace dimming_id here with either 0 or 1

    df["has_dimming"] = df["dimming_id"].apply(lambda x: 0 if pd.isnull(x) else 1)

    sorted_df = df.sort_values(by=["has_dimming", "flare_class_score"], ascending=False)

    # Apply verif_level_from_row to each row

    df["verif_level"] = df.apply(verif_level_from_row, axis=1)

    sorted_df = df.sort_values(by=["verif_level"], ascending=True)

    # But we don't want verification level -1

    sorted_df = sorted_df[sorted_df["verif_level"] != -1]

    # And if this is empty, well there's no match so we continue

    if sorted_df.empty:
        continue

    # Otherwise we can continue

    top_choice = sorted_df.iloc[0]

    verification_level = top_choice["verif_level"]
    harpnum = top_choice["harpnum"]

    # There's a match!
    matches[unique_cme[0]] = {
        "harpnum": harpnum,
        "verification_level": verification_level,
    }

new_cur.execute("DELETE FROM FINAL_CME_HARP_ASSOCIATIONS")

# Iterate through key, value pairs of matches
for cme_id, values in tqdm(matches.items()):
    harpnum = int(values["harpnum"])
    verification_score = int(values["verification_level"])

    association_method = "automatic"
    independent_verfied = 0

    # Add to database
    new_cur.execute(
        "INSERT INTO FINAL_CME_HARP_ASSOCIATIONS (cme_id, harpnum, verification_score, association_method, independent_verified) VALUES (?, ?, ?, ?, ?)",
        (cme_id, harpnum, verification_score, association_method, independent_verfied),
    )

new_conn.commit()
