from src.cmesrc.utils import read_SWAN_filepath, filepaths_updated_swan_data
import sys
from tqdm import tqdm
import pandas as pd
import sqlite3
import os


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from src.cmesrc.config import (
    CMESRC_BBOXES,
    CMESRC_DB,
    HARPNUM_TO_NOAA,
)

print("Pre-data loading script")

# If this is being called by make all is because we need to recreate the database
if os.path.exists(CMESRC_BBOXES):
    os.remove(CMESRC_BBOXES)
if os.path.exists(CMESRC_DB):
    os.remove(CMESRC_DB)

# Create CMESRC_BBOXES
new_conn = sqlite3.connect(CMESRC_BBOXES)
new_cur = new_conn.cursor()

new_cur.executescript(
    """
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
"""
)

new_conn.commit()

# Now read the data

swan_filepaths = filepaths_updated_swan_data()
new_cur.execute("DELETE FROM RAW_HARPS_BBOX;")

# tqdm should clear after finishing
for harpnum in tqdm(
    swan_filepaths.keys(), desc="Reading SWAN data", unit="HARP", leave=False
):
    filepath = swan_filepaths[harpnum]
    data = read_SWAN_filepath(filepath)
    # Replace the NaN values with 0
    data["IS_TMFI"] = data["IS_TMFI"].fillna(0)
    bbox_data = [
        (
            int(harpnum),
            str(row["Timestamp"])[:-4],
            float(row["LONDTMIN"]),
            float(row["LONDTMAX"]),
            float(row["LATDTMIN"]),
            float(row["LATDTMAX"]),
            int(row["IRBB"]),
            int(row["IS_TMFI"]),
        )
        for i, row in data.iterrows()
    ]

    # Insert the data into the database RAW_HARPS_BBOX table
    new_cur.executemany(
        """
    INSERT INTO RAW_HARPS_BBOX (harpnum, timestamp, LONDTMIN, LONDTMAX, LATDTMIN, LATDTMAX, IRBB, IS_TMFI)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """,
        bbox_data,
    )

new_conn.commit()

new_cur.execute("DELETE FROM HARPS;")
new_cur.execute(
    "SELECT harpnum, min(timestamp) as start, max(timestamp) as end FROM RAW_HARPS_BBOX GROUP BY harpnum;"
)

data = new_cur.fetchall()

data = [(int(row[0]), str(row[1]), str(row[2])) for row in data]

for row in data:
    new_cur.execute(
        """
    INSERT INTO HARPS (harpnum, start, end)
    VALUES (?, ?, ?);
    """,
        row,
    )

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
            new_cur.execute(
                """
                INSERT INTO NOAA_HARPNUM_MAPPING (noaa, harpnum)
                VALUES (?, ?)
            """,
                (int(noaa), int(row["HARPNUM"])),
            )
        except sqlite3.IntegrityError as e:
            print(f"Integrity error for {noaa}, {row['HARPNUM']}")

new_cur.execute(
    """
INSERT INTO NOAAS (noaa)
SELECT DISTINCT noaa
FROM NOAA_HARPNUM_MAPPING
"""
)

# Now we get to calculating areas and overlaps

clear_screen()
print("Data pre-loading: Calculating areas and overlaps")

new_cur.execute(
    """
WITH AREAS AS (
                SELECT harpnum,
                COALESCE(AVG(NULLIF(100.0 * ((PI()/180.0 * (LONDTMAX - LONDTMIN) * ABS(SIN(PI()/180.0 * LATDTMAX) - SIN(PI()/180.0 * LATDTMIN))) / (2.0 * PI())), 0)),0) AS area
                FROM RAW_HARPS_BBOX
                GROUP BY harpnum
                )
UPDATE HARPS
SET area = (SELECT area FROM AREAS WHERE AREAS.harpnum = HARPS.harpnum);
                """
)

new_cur.execute(
    """
UPDATE HARPS
SET n_noaas = (SELECT COUNT(noaa)
               FROM NOAA_HARPNUM_MAPPING
               WHERE NOAA_HARPNUM_MAPPING.harpnum = HARPS.harpnum)
    """
)


new_conn.commit()

# Calculating the overlaps is a step by step process
# 1. We remove too big harps

new_cur.execute("CREATE INDEX IF NOT EXISTS idx_harps_area ON HARPS (area);")
new_cur.execute("CREATE INDEX IF NOT EXISTS idx_harps_harpnum ON HARPS (area);")

new_cur.execute("DROP TABLE IF EXISTS NO_BIG_HARPS;")
new_cur.execute(
    """
CREATE TEMPORARY TABLE NO_BIG_HARPS AS
                SELECT RHB.* FROM RAW_HARPS_BBOX RHB
                INNER JOIN HARPS H ON RHB.harpnum = H.harpnum
                WHERE H.area < 18
                """
)

# 2. We trim the bounding boxes that extend beyond the limb

new_cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_no_big_harps_harpnum ON NO_BIG_HARPS (LONDTMIN);"
)
new_cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_no_big_harps_harpnum ON NO_BIG_HARPS (LONDTMAX);"
)

new_cur.execute("DROP TABLE IF EXISTS TRIMMED_HARPS_BBOX;")
new_cur.execute(
    """
CREATE TEMPORARY TABLE TRIMMED_HARPS_BBOX AS
SELECT NBH.* FROM NO_BIG_HARPS NBH
"""
)

# Now whenever LONDTMIN < -90 we set it to -90
# Whenever LONDTMAX > 90 we set it to 90
# And if LONTMIN<-90 AND LONDTMIN < -90 we drop that row
# And if LONTMIN>90 AND LONDTMIN > -90 we drop that row

new_cur.execute(
    """
DELETE FROM TRIMMED_HARPS_BBOX
WHERE (LONDTMIN < -90 AND LONDTMAX < -90)
OR (LONDTMIN > 90 AND LONDTMAX > 90)
"""
)

new_cur.execute(
    """
UPDATE TRIMMED_HARPS_BBOX
SET LONDTMIN = -90
WHERE LONDTMIN < -90
"""
)

new_cur.execute(
    """
UPDATE TRIMMED_HARPS_BBOX
SET LONDTMAX = 90
WHERE LONDTMAX > 90
"""
)

# 3. Now we need to calculate the actual overlaps

print("Calculating overlaps...")
new_cur.executescript(
    """
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
"""
)

new_conn.commit()

overlaps = pd.read_sql_query(
    """
                             SELECT O.*, H1.area as harpnum_a_area, H2.area as harpnum_b_area, H2.area / H1.area as b_over_a_area_ratio FROM OVERLAPS O
                             INNER JOIN HARPS H1 ON O.harpnum_a = H1.harpnum
                             INNER JOIN HARPS H2 ON O.harpnum_b = H2.harpnum
                             """,
    new_conn,
)

bad_overlaps = overlaps[
    ((overlaps["mean_overlap"] > 50) & (overlaps["ocurrence_percentage"] > 50))
    | (overlaps["mean_overlap"] == 100)
]

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

    new_cur.execute(
        "INSERT OR IGNORE INTO OVERLAP_RECORDS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            harpnum_a,
            harpnum_b,
            decision,
            mean_overlap,
            row["std_overlap"],
            occurence_percentage,
            harpnum_a_area,
            harpnum_b_area,
            row["b_over_a_area_ratio"],
        ),
    )

new_conn.commit()

print("Creating processed_harps_bbox table...")
# Now fill processed_harps_bbox

new_cur.executescript(
    """
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
"""
)

new_conn.commit()

clear_screen()
