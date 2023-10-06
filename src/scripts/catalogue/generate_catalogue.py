import sys

import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
import subprocess
import sqlite3
from tqdm import tqdm
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
import bisect

from src.cmesrc.config import CMESRCV2_DB, CMESRCV3_DB, HARPNUM_TO_NOAA, LASCO_CME_DATABASE, SDOML_TIMESTAMP_INFO,SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE, DIMMINGS_MATCHED_TO_HARPS_PICKLE, FLARES_MATCHED_TO_HARPS_PICKLE
from src.cmesrc.utils import read_SWAN_filepath, filepaths_updated_swan_data

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

FORCE = True
RERUN_CATALOGUE_SCRIPTS = False
N_STEPS = 5

clear_screen()
print("Step 1 of {}: Create dataset and load data".format(N_STEPS))

# Delete CMESRCV3_DB if exists
if os.path.exists(CMESRCV3_DB):
    if not FORCE:
        raise Exception("CMESRCV3_DB already exists. Set FORCE=True to overwrite.")
    else:
        os.remove(CMESRCV3_DB)

# Create CMESRCV3_DB
new_conn = sqlite3.connect(CMESRCV3_DB)
new_cur = new_conn.cursor()

new_cur.executescript("""
CREATE TABLE HARPS (
  harpnum INTEGER PRIMARY KEY,                    -- Unique identifier for each HARP region
  start TEXT NOT NULL, -- Start timestamp of the HARP region
  end TEXT NOT NULL,  -- End timestamp of the HARP region
  area FLOAT, -- Refers to the area of the HARP region
  n_noaas INTEGER, -- Refers to the maximum number of NOAA active regions in the HARP region
  CHECK (area >= 0),
  CHECK (end >= start)
);
CREATE TABLE CMES (
  cme_id INTEGER NOT NULL PRIMARY KEY,            -- Unique identifier for each CME
  cme_date TEXT NOT NULL,                         -- Date and time of the CME
  cme_pa REAL,                                    -- Position angle of the CME
  cme_width REAL NOT NULL,                        -- Width of the CME
  cme_halo INTEGER,                               -- Indicator for Halo CMEs (1 for Halo, else NULL)
  cme_seen_in INTEGER NOT NULL,                   -- Where the CME was observed (e.g., C2, C3)
  cme_three_points INTEGER NOT NULL,              -- Number of observed points for the CME
  cme_quality INTEGER NOT NULL,                   -- Quality rating for the CME observation
  image_timestamp TEXT, -- Timestamp of the associated image
  CHECK (
    ((cme_pa IS NULL) = (cme_halo = 1))           -- Ensure Halo CMEs don't have a position angle
  )
);
CREATE TABLE FLARES (
  flare_id INTEGER NOT NULL PRIMARY KEY,          -- Unique identifier for each flare
  harpnum INTEGER REFERENCES HARPS (harpnum),     -- Associated HARP region
  flare_date TEXT NOT NULL,                       -- Date and time of the flare
  flare_lon REAL,                                 -- Longitude of the flare
  flare_lat REAL,                                 -- Latitude of the flare
  flare_class_score REAL NOT NULL,                -- Score indicating the class of the flare
  flare_class TEXT NOT NULL,                      -- Class of the flare (e.g., M1, X2)
  flare_ar INTEGER,                               -- Active Region number of the flare
  flare_ar_source TEXT,                           -- Source of the Active Region number
  flare_verification TEXT,                        -- Verification status of the flare information
  image_timestamp TEXT, -- Timestamp of the associated image
  CHECK (
    (flare_ar IS NULL AND flare_lon IS NOT NULL AND flare_lat IS NOT NULL) OR
    (flare_ar IS NOT NULL)                        -- Ensure either flare_ar or both flare_lon and flare_lat are provided
  )
);
CREATE TABLE RAW_HARPS_BBOX (
    -- Reference to the harpnum column in the harps table
    harpnum INTEGER REFERENCES HARPS (harpnum), 
    -- Reference to the timestamp column in the images table
    timestamp TEXT, 
    -- Minimum longitude of BBOX
    LONDTMIN REAL, 
    -- Maximum longitude of BBOX
    LONDTMAX REAL, 
    -- Minimum latitude of BBOX
    LATDTMIN REAL, 
    -- Maximum latitude of BBOX
    LATDTMAX REAL, 
    -- Is Rotated Bounding Box? Specifies if the BBOX is calculated using differential solar rotation because it was missing from the SHARP dataset
    IRBB INTEGER, 
    -- Is trusted magnetic field data?
    IS_TMFI INTEGER,
    PRIMARY KEY (harpnum, timestamp),
    -- Need to check that the timestamp has minutes 12, 24, 36, 48, or 00
    CHECK (strftime('%M', timestamp) IN ('12', '24', '36', '48', '00'))
);
CREATE TABLE FINAL_CME_HARP_ASSOCIATIONS (
  cme_id INTEGER UNIQUE NOT NULL REFERENCES CMES(cme_id),                 -- Unique identifier for each CME
  harpnum INTEGER NOT NULL REFERENCES HARPS(harpnum),                       -- Associated HARP region
  association_method TEXT NOT NULL,               -- Method used to determine the association
  verification_score REAL,                        -- Confidence score of the association
  independent_verified INTEGER NOT NULL DEFAULT 0, -- Verification level for association (0 if only by me, 1 if verified by external)
  PRIMARY KEY (cme_id, harpnum)                   -- Unique pairing of CME and HARP
);
CREATE TABLE OVERLAPS (
    harpnum_a INTEGER REFERENCES HARPS(harpnum),
    harpnum_b INTEGER REFERENCES HARPS(harpnum),
    mean_overlap REAL,
    mean_pixel_overlap REAL,
    ocurrence_percentage REAL,
    pixel_ocurrence_percentage REAL,
    harpnum_a_area REAL,
    harpnum_a_pixel_area REAL,
    harpnum_b_area REAL,
    coexistence REAL
);
CREATE TABLE OVERLAP_RECORDS (
            harpnum_a INTEGER REFERENCES HARPS(harpnum),
            harpnum_b INTEGER REFERENCES HARPS(harpnum),
            decision STRING,
            mean_overlap REAL,
            std_overlap REAL,
            ocurrence_percentage REAL,
            harpnum_a_area REAL,
            harpnum_b_area REAL,
            b_over_a_area_ratio REAL,
            PRIMARY KEY (harpnum_a, harpnum_b),
            CHECK (harpnum_a_area < harpnum_b_area),
            CHECK (decision IN ('MERGED A WITH B', 'DELETED A IN FAVOR OF B'))
);
CREATE TABLE PROCESSED_HARPS_BBOX(
    -- Reference to the harpnum column in the harps table
    harpnum INTEGER REFERENCES HARPS (harpnum), 
    -- Reference to the timestamp column in the images table
    timestamp TEXT, 
    -- Minimum longitude of BBOX
    LONDTMIN REAL, 
    -- Maximum longitude of BBOX
    LONDTMAX REAL, 
    -- Minimum latitude of BBOX
    LATDTMIN REAL, 
    -- Maximum latitude of BBOX
    LATDTMAX REAL, 
    -- Is Rotated Bounding Box? Specifies if the BBOX is calculated using differential solar rotation because it was missing from the SHARP dataset
    IRBB INTEGER, 
    -- Is trusted magnetic field data?
    IS_TMFI INTEGER,
    PRIMARY KEY (harpnum, timestamp),
    -- Need to check that the timestamp has minutes 12, 24, 36, 48, or 00
    CHECK (strftime('%M', timestamp) IN ('12', '24', '36', '48', '00'))
);
CREATE TABLE IF NOT EXISTS CMES_HARPS_EVENTS (
	harpnum	INTEGER,
	cme_id	INTEGER,
	flare_id	INTEGER REFERENCES FLARES(flare_id),
	flare_hours_diff	INTEGER NOT NULL,
	dimming_id	INTEGER REFERENCES DIMMINGS(dimming_id),
	dimming_hours_diff	INTEGER NOT NULL,
	FOREIGN KEY(harpnum,cme_id) REFERENCES CMES_HARPS_SPATIALLY_CONSIST(harpnum,cme_id),
	PRIMARY KEY(harpnum,cme_id)
);
CREATE TABLE IF NOT EXISTS CMES_HARPS_SPATIALLY_CONSIST (
	harpnum	INTEGER NOT NULL REFERENCES HARPS(harpnum),
	cme_id	INTEGER NOT NULL REFERENCES CMES(cme_id),
	PRIMARY KEY(harpnum,cme_id)
);
CREATE TABLE IF NOT EXISTS DIMMINGS (
	dimming_id	INTEGER NOT NULL,
	harpnum	INTEGER REFERENCES HARPS(harpnum),
	harps_dimming_dist	REAL NOT NULL,
	dimming_start_date	TEXT NOT NULL,
	dimming_peak_date	TEXT NOT NULL,
	dimming_lon	REAL NOT NULL,
	dimming_lat	REAL NOT NULL,
	image_timestamp	TEXT,
	PRIMARY KEY(dimming_id)
);
CREATE TABLE NOAA_HARPNUM_MAPPING (
        noaa INTEGER,
        harpnum INTEGER REFERENCES HARPS (harpnum),
        PRIMARY KEY (noaa, harpnum)
        );
CREATE TABLE NOAAS (
    noaa INTEGER PRIMARY KEY
);
""")

new_conn.commit()

# Now read the data

swan_filepaths = filepaths_updated_swan_data()
new_cur.execute("DELETE FROM RAW_HARPS_BBOX;")

# tqdm should clear after finishing
for harpnum in tqdm(swan_filepaths.keys(), desc="Reading SWAN data", unit="HARP", leave=False):
    filepath = swan_filepaths[harpnum]
    data = read_SWAN_filepath(filepath)
    # Replace the NaN values with 0
    data["IS_TMFI"] = data["IS_TMFI"].fillna(0)
    data = [(int(harpnum), str(row["Timestamp"])[:-4], float(row["LONDTMIN"]), float(row["LONDTMAX"]), float(row["LATDTMIN"]), float(row["LATDTMAX"]), int(row["IRBB"]), int(row["IS_TMFI"])) for i, row in data.iterrows()]

    # Insert the data into the database RAW_HARPS_BBOX table
    new_cur.executemany("""
    INSERT INTO RAW_HARPS_BBOX (harpnum, timestamp, LONDTMIN, LONDTMAX, LATDTMIN, LATDTMAX, IRBB, IS_TMFI)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, data)
new_conn.commit()

new_cur.execute("DELETE FROM HARPS;")
new_cur.execute("SELECT harpnum, min(timestamp) as start, max(timestamp) as end FROM RAW_HARPS_BBOX GROUP BY harpnum;")

data = new_cur.fetchall()

data = [(int(row[0]), str(row[1]), str(row[2])) for row in data]

for row in data:
    new_cur.execute("""
    INSERT INTO HARPS (harpnum, start, end)
    VALUES (?, ?, ?);
    """, row)
        
new_conn.commit()

# Now read HARPs-NOAA mapping

new_cur.execute("DELETE FROM NOAA_HARPNUM_MAPPING;")
new_cur.execute("DELETE FROM NOAAS;")

noaatoharp = pd.read_csv(HARPNUM_TO_NOAA, sep=" ", header=0)

def parse_noaa_lists(noaa_list):
    return [int(noaa) for noaa in noaa_list.split(",")]

noaatoharp["noaa_list"] = noaatoharp["NOAA_ARS"].apply(parse_noaa_lists)

for _, row in noaatoharp.iterrows():
    for noaa in row["noaa_list"]:
        if int(row["HARPNUM"]) > 7331.5:
            continue
        try:
            new_cur.execute("""
                INSERT INTO NOAA_HARPNUM_MAPPING (noaa, harpnum)
                VALUES (?, ?)
            """, (int(noaa), int(row["HARPNUM"])))
        except sqlite3.IntegrityError as e:
            print(f"Integrity error for {noaa}, {row['HARPNUM']}")

new_cur.execute("""
INSERT INTO NOAAS (noaa)
SELECT DISTINCT noaa
FROM NOAA_HARPNUM_MAPPING
""")

# Now we get to calculating areas and overlaps

clear_screen()
print("Step 2 of {}: Calculating areas and overlaps".format(N_STEPS))

new_cur.execute("""
WITH AREAS AS (
                SELECT harpnum,
                COALESCE(AVG(NULLIF(100.0 * ((PI()/180.0 * (LONDTMAX - LONDTMIN) * ABS(SIN(PI()/180.0 * LATDTMAX) - SIN(PI()/180.0 * LATDTMIN))) / (2.0 * PI())), 0)),0) AS area
                FROM RAW_HARPS_BBOX
                GROUP BY harpnum
                )
UPDATE HARPS
SET area = (SELECT area FROM AREAS WHERE AREAS.harpnum = HARPS.harpnum);
                """)

new_cur.execute("""
UPDATE HARPS
SET n_noaas = (SELECT COUNT(noaa)
               FROM NOAA_HARPNUM_MAPPING
               WHERE NOAA_HARPNUM_MAPPING.harpnum = HARPS.harpnum)
    """)


new_conn.commit()

# Calculating the overlaps is a step by step process
# 1. We remove too big harps

new_cur.execute("CREATE INDEX IF NOT EXISTS idx_harps_area ON HARPS (area);")
new_cur.execute("CREATE INDEX IF NOT EXISTS idx_harps_harpnum ON HARPS (area);")

new_cur.execute("DROP TABLE IF EXISTS NO_BIG_HARPS;")
new_cur.execute("""
CREATE TEMPORARY TABLE NO_BIG_HARPS AS
                SELECT RHB.* FROM RAW_HARPS_BBOX RHB
                INNER JOIN HARPS H ON RHB.harpnum = H.harpnum
                WHERE H.area < 18
                """)
    
# 2. We trim the bounding boxes that extend beyond the limb

new_cur.execute("CREATE INDEX IF NOT EXISTS idx_no_big_harps_harpnum ON NO_BIG_HARPS (LONDTMIN);")
new_cur.execute("CREATE INDEX IF NOT EXISTS idx_no_big_harps_harpnum ON NO_BIG_HARPS (LONDTMAX);")

new_cur.execute("DROP TABLE IF EXISTS TRIMMED_HARPS_BBOX;")
new_cur.execute("""
CREATE TEMPORARY TABLE TRIMMED_HARPS_BBOX AS
SELECT NBH.* FROM NO_BIG_HARPS NBH
""")

# Now whenever LONDTMIN < -90 we set it to -90
# Whenever LONDTMAX > 90 we set it to 90
# And if LONTMIN<-90 AND LONDTMIN < -90 we drop that row
# And if LONTMIN>90 AND LONDTMIN > -90 we drop that row

new_cur.execute("""
DELETE FROM TRIMMED_HARPS_BBOX
WHERE (LONDTMIN < -90 AND LONDTMAX < -90)
OR (LONDTMIN > 90 AND LONDTMAX > 90)
""")

new_cur.execute("""
UPDATE TRIMMED_HARPS_BBOX
SET LONDTMIN = -90
WHERE LONDTMIN < -90
""")

new_cur.execute("""
UPDATE TRIMMED_HARPS_BBOX
SET LONDTMAX = 90
WHERE LONDTMAX > 90
""")

# 3. Now we need to calculate the actual overlaps

print("Calculating overlaps...")
new_cur.executescript("""
-- Create an index on HARPS_BBOX for the join in the temp_overlap creation step
CREATE INDEX IF NOT EXISTS idx_thbb_harpnum_timestamp ON TRIMMED_HARPS_BBOX(harpnum, timestamp);

-- Create temporary table with overlap info
CREATE TEMPORARY TABLE temp_overlap AS
SELECT
    a.harpnum AS harpnum1,
    b.harpnum AS harpnum2,
    a.timestamp AS timestamp,
    100.0 * CASE WHEN a.LONDTMIN < b.LONDTMAX AND a.LONDTMAX > b.LONDTMIN AND a.LATDTMIN < b.LATDTMAX AND a.LATDTMAX > b.LATDTMIN
        THEN (MIN(a.LONDTMAX, b.LONDTMAX) - MAX(a.LONDTMIN, b.LONDTMIN)) * ABS((SIN(PI() / 180.0 * MIN(a.LATDTMAX, b.LATDTMAX)) - SIN(PI() / 180.0 * MAX(a.LATDTMIN, b.LATDTMIN))))
        ELSE 0
    END / ((2.0 * PI() / (100.0 * PI() / 180.0)) * H.area) AS overlap_percent
FROM TRIMMED_HARPS_BBOX a
JOIN TRIMMED_HARPS_BBOX b ON a.timestamp = b.timestamp AND a.harpnum != b.harpnum
JOIN HARPS H ON H.harpnum = a.harpnum;

-- Create an index on temp_overlap for the window function in the temp_largest_overlap creation step
CREATE INDEX IF NOT EXISTS idx_temp_overlap_harpnum1_timestamp_overlap ON temp_overlap(harpnum1, timestamp, overlap_percent DESC);

DROP TABLE IF EXISTS avg_overlap;
DROP TABLE IF EXISTS OVERLAPS;

CREATE TEMP TABLE avg_overlap AS
    SELECT tpo.harpnum1 as harpnum_a, tpo.harpnum2 as harpnum_b, AVG(tpo.overlap_percent) AS mean_overlap,
    (100.0 * (COUNT(tpo.timestamp) - 1) * 12.0 * 60.0) / (1.0 * NULLIF(strftime('%s', H.end) - strftime('%s', H.start), 0)) AS ocurrence_percentage
    FROM temp_overlap tpo
    JOIN HARPS H ON H.harpnum = tpo.harpnum1
    WHERE tpo.overlap_percent > 0
    GROUP BY harpnum1, harpnum2;

CREATE TABLE OVERLAPS AS
    SELECT ao.*,
    -- Standard deviation of the overlap
    SQRT(SUM((tpo.overlap_percent - ao.mean_overlap) * (tpo.overlap_percent - ao.mean_overlap)) / (COUNT(tpo.timestamp) - 1)) AS std_overlap
    FROM avg_overlap ao
    INNER JOIN TEMP_OVERLAP tpo ON ao.harpnum_a = tpo.harpnum1 AND ao.harpnum_b = tpo.harpnum2
    GROUP BY harpnum_a, harpnum_b;
""")

new_conn.commit()

overlaps = pd.read_sql_query("""
                             SELECT O.*, H1.area as harpnum_a_area, H2.area as harpnum_b_area, H2.area / H1.area as b_over_a_area_ratio FROM OVERLAPS O
                             INNER JOIN HARPS H1 ON O.harpnum_a = H1.harpnum
                             INNER JOIN HARPS H2 ON O.harpnum_b = H2.harpnum
                             """, new_conn)

bad_overlaps = overlaps[((overlaps["mean_overlap"] > 50) & (overlaps["ocurrence_percentage"] > 50)) | (overlaps["mean_overlap"] == 100)]

for index, row in bad_overlaps.iterrows():
    occurence_percentage = row["ocurrence_percentage"]
    mean_overlap = row["mean_overlap"]
    harpnum_a = row["harpnum_a"]
    harpnum_b = row["harpnum_b"]
    harpnum_a_area = row["harpnum_a_area"]
    harpnum_b_area = row["harpnum_b_area"]

    if occurence_percentage > 70 and mean_overlap > 90:
        decision = "MERGED A WITH B"
    else:
        decision = "DELETED A IN FAVOR OF B"
    
    new_cur.execute("INSERT OR IGNORE INTO OVERLAP_RECORDS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (harpnum_a, harpnum_b, decision, mean_overlap, row["std_overlap"], occurence_percentage, harpnum_a_area, harpnum_b_area, row["b_over_a_area_ratio"]))

new_conn.commit()

print("Creating processed_harps_bbox table...")
# Now fill processed_harps_bbox

new_cur.executescript("""
DROP TABLE IF EXISTS PROCESSED_HARPS_BBOX;

CREATE TABLE PROCESSED_HARPS_BBOX AS
                      SELECT RHB.* FROM RAW_HARPS_BBOX RHB
                      INNER JOIN HARPS H
                      ON RHB.harpnum = H.harpnum
                      WHERE H.area < 18 AND RHB.harpnum NOT IN (SELECT harpnum_a FROM OVERLAP_RECORDS);

DELETE FROM PROCESSED_HARPS_BBOX
WHERE (LONDTMIN < -90 AND LONDTMAX < -90)
OR (LONDTMIN > 90 AND LONDTMAX > 90);

UPDATE PROCESSED_HARPS_BBOX
SET LONDTMIN = -90
WHERE LONDTMIN < -90;

UPDATE PROCESSED_HARPS_BBOX
SET LONDTMAX = 90
WHERE LONDTMAX > 90;
""")

new_conn.commit()

clear_screen()

print("Step 3 of {}: Running association scripts".format(N_STEPS))

# Now, there are a number of scripts that need to be run to get
# the dimming and flares associations as well as the spatiotemporally
# consistent CME-HARP associations

# Option 1 is just to run make all and let it see if anything needs to be updated

if not RERUN_CATALOGUE_SCRIPTS:
    print("Running make all...")
    try:
        subprocess.run(["make", "all"], check=True)
    except subprocess.CalledProcessError as e:
        print("Error running make all. Please run make all manually.")
        print("Error message: {}".format(e))
        sys.exit(1)
else:
    # We first need to touch one of the early files so it's all rerun
    # Touch LASCO_CME_DATABASE to be edited now
    print("Rerunning everything...")
    os.utime(LASCO_CME_DATABASE, None)

    # And now we run make all
    try:
        subprocess.run(["make", "all"], check=True)
    except subprocess.CalledProcessError as e:
        print("Error running make all. Please run make all manually.")
        print("Error message: {}".format(e))
        sys.exit(1)

clear_screen()

print("Step 4 of {}: Loading script data into database".format(N_STEPS))
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
            row["CME_THREE_POINTS"]
        )
    )

# Spatially consistent CME-HARP associations

df = pd.read_pickle(SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE)
df = df[df["HARPS_SPAT_CONSIST"]]

for cme_id, harpnum in df[["CME_ID", "HARPNUM"]].values:
    cme_id = int(cme_id[2:])
    harpnum = int(harpnum)

    # Add the match to CMES_HARPS_SPATIALLY_CONSIST

    new_cur.execute("""
                INSERT INTO CMES_HARPS_SPATIALLY_CONSIST (harpnum, cme_id)
                VALUES (?, ?)
                """, (harpnum, cme_id))

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
            row["latitude"]
        )
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
            row["FLARE_VERIFICATION"]
        )
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

    candidate_timestamps.append(datetime_timestamp.replace(minute=0).replace(second=0) + timedelta(hours=1))

    # Sort them

    candidate_timestamps = sorted(candidate_timestamps)

    # Now we use bisect to find the closest minute

    new_timestamp = closest_timestamp(datetime_timestamp, candidate_timestamps)

    return new_timestamp


# Dimmings
for row in tqdm(new_cur.execute("SELECT dimming_id, dimming_start_date FROM dimmings").fetchall()):
    dimming_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(dimming_timestamp)
    new_cur.execute("UPDATE dimmings SET image_timestamp = ? WHERE dimming_id = ?", (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]))

# Same for flares
for row in tqdm(new_cur.execute("SELECT flare_id, flare_date FROM flares").fetchall()):
    flare_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(flare_timestamp)
    new_cur.execute("UPDATE flares SET image_timestamp = ? WHERE flare_id = ?", (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]))

# And for cmes
for row in tqdm(new_cur.execute("SELECT cme_id, cme_date FROM cmes").fetchall()):
    cme_timestamp = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
    image_timestamp = formatted_timestamp(cme_timestamp)
    new_cur.execute("UPDATE cmes SET image_timestamp = ? WHERE cme_id = ?", (image_timestamp.strftime("%Y-%m-%d %H:%M:%S"), row[0]))

new_conn.commit()

# Now do the associations
clear_screen()

print("Step 5 of {}: Associating dimmings, flares and CMEs".format(N_STEPS))

association_threshold = 2.01
min_time_before = 12 / 60

results = {}

harps = new_cur.execute("SELECT DISTINCT harpnum from PROCESSED_HARPS_BBOX").fetchall()

new_cur.execute("DELETE FROM CMES_HARPS_EVENTS")

for harp in tqdm(harps):
    harp = harp[0]

    flare_data = new_cur.execute("SELECT image_timestamp, flare_id, flare_class_score FROM FLARES WHERE harpnum = ? AND flare_verification != 'Non-verified'", (harp,)).fetchall()
    flare_timestamps, flare_ids, flare_class_scores = zip(*flare_data) if flare_data else ([], [], [])

    dimming_data = new_cur.execute("SELECT image_timestamp, dimming_id FROM DIMMINGS WHERE harpnum = ?", (harp,)).fetchall()
    dimming_timestamps, dimming_ids = zip(*dimming_data) if dimming_data else ([], [])

    present_at_cme_data = new_cur.execute("""
        SELECT c.image_timestamp, c.cme_id from CMES_HARPS_SPATIALLY_CONSIST as sch
        INNER JOIN CMES as c
        ON sch.cme_id = c.cme_id
        WHERE sch.harpnum = ? 
    """, (harp,)).fetchall()
    present_at_cme_timestamps, present_at_cme_ids = zip(*present_at_cme_data) if present_at_cme_data else ([], [])

    # Convert to datetime objects
    flare_timestamps = [datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in flare_timestamps]
    dimming_timestamps = [datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in dimming_timestamps]
    present_at_cme_timestamps = [datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in present_at_cme_timestamps]

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
                        matching_flares.append((hour_diff, flare_ids[flare_index], flare_class_scores[flare_index]))
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
                        matching_dimmings.append((hour_diff, dimming_ids[dimming_index]))
                elif hour_diff > association_threshold:
                    break

            if matching_dimmings:
                closest_dimming = min(matching_dimmings, key=lambda x: x[0])
                closest_dimming_hours_diff, closest_dimming_id = closest_dimming

        results[(harp, cme_id)] = {
            'closest_flare_id': closest_flare_id,
            'closest_flare_hours_diff': closest_flare_hours_diff,
            'closest_dimming_id': closest_dimming_id,
            'closest_dimming_hours_diff': closest_dimming_hours_diff
        }

for (harp, cme_id), event in tqdm(results.items()):
    new_cur.execute("""
        INSERT INTO CMES_HARPS_EVENTS (harpnum, cme_id, flare_id, flare_hours_diff, dimming_id, dimming_hours_diff)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (harp, cme_id, event['closest_flare_id'], event['closest_flare_hours_diff'], event['closest_dimming_id'], event['closest_dimming_hours_diff']))

new_conn.commit()

# Now in order to find every match we can choose all rows for each CME and sort
# first by which have a dimming and then by flare class

# Let's see how many potential matches there are

unique_cmes = new_cur.execute("SELECT DISTINCT cme_id from CMES_HARPS_SPATIALLY_CONSIST").fetchall()

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
    has_dimming = row['has_dimming']
    has_flare = 0 if pd.isnull(row['flare_id']) else 1
    flare_class = None if pd.isnull(row['flare_class_score']) else row['flare_class_score']
    harpnum = row['harpnum']

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

    df['has_dimming'] = df['dimming_id'].apply(lambda x: 0 if pd.isnull(x) else 1)

    sorted_df = df.sort_values(by=['has_dimming', 'flare_class_score'], ascending=False)

    # Apply verif_level_from_row to each row

    df["verif_level"] = df.apply(verif_level_from_row, axis=1)

    sorted_df = df.sort_values(by=['verif_level'], ascending=True)

    # But we don't want verification level -1

    sorted_df = sorted_df[sorted_df['verif_level'] != -1]

    # And if this is empty, well there's no match so we continue

    if sorted_df.empty:
        continue

    # Otherwise we can continue

    top_choice = sorted_df.iloc[0]

    verification_level = top_choice['verif_level']
    harpnum = top_choice['harpnum']

    # There's a match!
    matches[unique_cme[0]] = {
        'harpnum': harpnum,
        'verification_level': verification_level
    }

new_cur.execute("DELETE FROM FINAL_CME_HARP_ASSOCIATIONS")

# Iterate through key, value pairs of matches
for cme_id, values in tqdm(matches.items()):
    harpnum = int(values["harpnum"])
    verification_score = int(values["verification_level"])

    association_method = "automatic"
    independent_verfied = 0

    # Add to database
    new_cur.execute("INSERT INTO FINAL_CME_HARP_ASSOCIATIONS (cme_id, harpnum, verification_score, association_method, independent_verified) VALUES (?, ?, ?, ?, ?)", (cme_id, harpnum, verification_score, association_method, independent_verfied))

new_conn.commit()