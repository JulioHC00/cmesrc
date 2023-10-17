import sys
sys.path.append('/home/julio/cmesrc/')

from src.cmesrc.config import CMESRCV3_DB, PIXEL_BBOXES, SDOML_DATASET, GENERAL_DATASET, RESEARCH_LOG, ZARR_BASE_PATH, SDOML_TIMESTAMP_INFO
import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm
import networkx as nx
import seaborn as sns
import zarr
from tqdm import tqdm
import pickle
import gc

# Reset everything

# Check if SDOML dataset exists
if os.path.exists(SDOML_DATASET):
    os.remove(SDOML_DATASET)

# Create SDOML dataset as a copy of GENERAL_DATASET
os.system(f"cp {GENERAL_DATASET} {SDOML_DATASET}")

# Connect

conn = sqlite3.connect(SDOML_DATASET)
cur = conn.cursor()

# Read RAW pixel bounding boxes

conn.execute("DROP TABLE IF EXISTS RAW_HARPS_PIXEL_BBOX;")

# Need to attach the pixel database
conn.execute("ATTACH DATABASE ? AS pixel_values", (PIXEL_BBOXES,))

# Now we need to find the pixel data for each image

conn.execute("""
CREATE TABLE main.RAW_HARPS_PIXEL_BBOX AS
                 SELECT PHPB.harpnum, PHPB.timestamp, PHPB.x_min, PHPB.x_max, PHPB.y_min, PHPB.y_max, PHPB.x_cen, PHPB.y_cen FROM pixel_values.HARPS_PIXEL_BBOX PHPB
                 """)

# Detach the old database

conn.execute("DETACH DATABASE pixel_values")
conn.commit()

# Get max widths and heights and update HARPS

try:
    cur.executescript("""
    ALTER TABLE HARPS ADD COLUMN pix_width INTEGER;
    ALTER TABLE HARPS ADD COLUMN pix_height INTEGER;
    """)
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        pass
    else:
        raise e

cur.execute("DROP TABLE IF EXISTS temp_aggregate;")

conn.executescript("""
CREATE TEMP TABLE temp_aggregate AS
SELECT harpnum, MAX(x_max - x_min) as max_width, MAX(y_max - y_min) as max_height
FROM RAW_HARPS_PIXEL_BBOX
GROUP BY harpnum;

UPDATE HARPS
SET pix_width = (
SELECT max_width + ABS(max_width % 2 - 1)
FROM temp_aggregate
WHERE HARPS.harpnum = temp_aggregate.harpnum
),
pix_height = (
SELECT max_height + ABS(max_height % 2 - 1)
FROM temp_aggregate
WHERE HARPS.harpnum = temp_aggregate.harpnum
);

DROP TABLE temp_aggregate;

ALTER TABLE HARPS ADD COLUMN pix_area INTEGER;

UPDATE HARPS
SET pix_area = pix_width * pix_height;
""")

# Create processed pixel bounding boxes
# Using the max width and height

conn.execute("DROP TABLE IF EXISTS PROCESSED_HARPS_PIXEL_BBOX;")

# Then we need to update the values of x_min, x_max, y_min, y_max in PROCESSED_HARPS_PIXEL_BBOX

conn.executescript("""
                    CREATE TABLE IF NOT EXISTS PROCESSED_HARPS_PIXEL_BBOX AS
                    SELECT PHPB.harpnum, PHPB.timestamp, 
                    PHPB.x_cen - ((H.pix_width - 1) / 2) AS x_min,
                    PHPB.x_cen + ((H.pix_width - 1) / 2 + 1) AS x_max,
                    PHPB.y_cen - ((H.pix_height - 1) / 2) AS y_min,
                    PHPB.y_cen + ((H.pix_height - 1) / 2 + 1) AS y_max,
                    PHPB.x_cen, PHPB.y_cen
                    FROM RAW_HARPS_PIXEL_BBOX PHPB
                    INNER JOIN HARPS H
                    ON PHPB.harpnum = H.harpnum
                    """)

conn.commit()

# Create the hourly pixel bounding boxes

cur.execute("""
CREATE TABLE HOURLY_PIXEL_BBOX AS
SELECT * FROM PROCESSED_HARPS_PIXEL_BBOX PHPBB
WHERE strftime("%M", timestamp) IN ("00", "12", "24")
GROUP BY harpnum, strftime("%Y %m %d %H", timestamp)
""")

conn.commit()

# Calculate average overlaps

cur.execute("DROP TABLE IF EXISTS temp_pixel_overlap")

cur.execute("""
CREATE TEMPORARY TABLE temp_pixel_overlap AS
SELECT
    a.harpnum AS harpnum1,
    b.harpnum AS harpnum2,
    a.timestamp AS timestamp,
    100.0 * CASE WHEN a.x_min < b.x_max AND a.x_max > b.x_min AND a.y_min < b.y_max AND a.y_max > b.y_min
        THEN ((MIN(a.x_max, b.x_max) - MAX(a.x_min, b.x_min)) * (MIN(a.y_max, b.y_max) - MAX(a.y_min, b.y_min)))
        ELSE 0
    END / (1.0 * H.pix_area) AS overlap_percent
FROM PROCESSED_HARPS_PIXEL_BBOX a
JOIN PROCESSED_HARPS_PIXEL_BBOX b ON a.timestamp = b.timestamp AND a.harpnum != b.harpnum
JOIN HARPS H ON H.harpnum = a.harpnum;
""")

cur.execute("DROP TABLE IF EXISTS avg_pixel_overlap")
cur.execute("DROP TABLE IF EXISTS PIXEL_OVERLAPS")

cur.executescript("""
CREATE TEMP TABLE avg_pixel_overlap AS
    SELECT tpo.harpnum1 as harpnum_a, tpo.harpnum2 as harpnum_b, AVG(tpo.overlap_percent) AS mean_overlap,
    (100.0 * (CASE WHEN COUNT(tpo.timestamp) > 1
             THEN (COUNT(tpo.timestamp) - 1)
             ELSE (COUNT(tpo.timestamp))
             END
             * 12.0 * 60.0) / (1.0 * NULLIF(strftime('%s', H.end) - strftime('%s', H.start), 0))) AS ocurrence_percentage
    FROM temp_pixel_overlap tpo
    JOIN HARPS H ON H.harpnum = tpo.harpnum1
    WHERE tpo.overlap_percent > 0
    GROUP BY harpnum1, harpnum2;

CREATE TABLE PIXEL_OVERLAPS AS
    SELECT ao.*,
    -- Standard deviation of the overlap
    SQRT(SUM((tpo.overlap_percent - ao.mean_overlap) * (tpo.overlap_percent - ao.mean_overlap)) / CASE WHEN COUNT(tpo.timestamp) > 1 THEN (COUNT(tpo.timestamp) - 1) ELSE 1 END) AS std_overlap
    FROM avg_pixel_overlap ao
    INNER JOIN temp_pixel_overlap tpo ON ao.harpnum_a = tpo.harpnum1 AND ao.harpnum_b = tpo.harpnum2
    GROUP BY harpnum_a, harpnum_b;
""")

conn.commit()

# UPDATE PIXEL OVERLAPS SO THEY INCLUDE
# INFO ABOUT CME ASSOCIATIONS

# Add columns harpnum_a_cme and harpnum_b_cme to PIXEL_OVERLAPS

try:
    cur.executescript("""
    ALTER TABLE PIXEL_OVERLAPS ADD COLUMN harpnum_a_cme INTEGER;
    ALTER TABLE PIXEL_OVERLAPS ADD COLUMN harpnum_b_cme INTEGER;
    """)
# Maybe they already exist
except sqlite3.OperationalError:
    pass

cur.execute("""
WITH cme_info AS (
SELECT 
            PO.harpnum_a, 
            PO.harpnum_b,
            CASE WHEN MAX(FCHA.harpnum) IS NULL THEN 0 ELSE 1 END AS harpnum_a_cme,
            CASE WHEN MAX(FCHA2.harpnum) IS NULL THEN 0 ELSE 1 END AS harpnum_b_cme,
			mean_overlap,
			ocurrence_percentage

FROM PIXEL_OVERLAPS PO
LEFT JOIN FINAL_CME_HARP_ASSOCIATIONS FCHA ON
PO.harpnum_a = FCHA.harpnum
LEFT JOIN FINAL_CME_HARP_ASSOCIATIONS FCHA2 ON
PO.harpnum_b = FCHA2.harpnum
GROUP BY PO.harpnum_a, PO.harpnum_b)

UPDATE PIXEL_OVERLAPS
SET harpnum_a_cme = (
SELECT harpnum_a_cme FROM cme_info
WHERE PIXEL_OVERLAPS.harpnum_a = cme_info.harpnum_a AND PIXEL_OVERLAPS.harpnum_b = cme_info.harpnum_b
),
harpnum_b_cme = (
SELECT harpnum_b_cme FROM cme_info
WHERE PIXEL_OVERLAPS.harpnum_a = cme_info.harpnum_a AND PIXEL_OVERLAPS.harpnum_b = cme_info.harpnum_b
)
            """)

conn.commit()

# Create ignore harps table using the overlap info

cur.execute("DROP TABLE IF EXISTS ignore_harps")
cur.execute("""
CREATE TEMP TABLE ignore_harps AS
    SELECT 
       CASE 
           WHEN harpnum_a_cme THEN harpnum_b 
           ELSE harpnum_a 
       END AS to_ignore 
FROM PIXEL_OVERLAPS
WHERE harpnum_a_cme != harpnum_b_cme 
  AND mean_overlap > 30 
  AND ocurrence_percentage > 30
ORDER BY harpnum_a, harpnum_b ASC;
""") 

# Select the accepted harps
# Conditions are
# 1. harpnum is not in ignore_harps
# 2. pix_area > 256
# 4. harpnum is in the HOURLY_PIXEL_BBOX table

cur.execute("DROP TABLE IF EXISTS ACCEPTED_HARPS")
cur.execute("""
CREATE TABLE ACCEPTED_HARPS AS
            SELECT DISTINCT H.harpnum FROM HARPS H
            INNER JOIN HOURLY_PIXEL_BBOX HPB ON H.harpnum = HPB.harpnum
            LEFT JOIN ignore_harps ih ON H.harpnum = ih.to_ignore
            WHERE H.pix_area > 256 AND ih.to_ignore IS NULL
""")

conn.commit()

# Add the zarr indices to the HOURLY_PIXEL_BBOX table

try:
    cur.execute("ALTER TABLE HOURLY_PIXEL_BBOX ADD COLUMN zarr_index INTEGER;")
except sqlite3.OperationalError as e:
    if "duplicate column name: zarr_index" == str(e):
        pass
    else:
        raise e

# Select all harpnums
cur.execute("""
SELECT DISTINCT harpnum FROM HOURLY_PIXEL_BBOX
""")

harpnums = cur.fetchall()

# Create index for harpnum, index for 
# faster access

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_hourly_pixel_bbox_harpnum_timestamp ON PROCESSED_HARPS_PIXEL_BBOX (harpnum, timestamp)
""")

cur.execute("DROP TABLE IF EXISTS temp_zarr_index")
cur.execute("""
CREATE TEMP TABLE temp_zarr_index (
    harpnum INTEGER,
    timestamp TEXT,
    zarr_index INTEGER
)
            """)

# Now we go through each harpnum and find the zarr index for each timestamp
for harpnum in tqdm(harpnums, desc="Finding zarr index", unit="harpnum"):
    harpnum = harpnum[0]

    # Open the zarr DirectoryStore
    zarr_path = os.path.join(ZARR_BASE_PATH, f"{harpnum}")

    zarr_store = zarr.DirectoryStore(zarr_path)

    # Open the zarr

    try:
        root = zarr.open(zarr_store, mode='r')
    except:
        print(f"Could not open zarr for harpnum {harpnum}")
        print(zarr_path)
        continue

    # Get the timestamps from the zarr group
    attrs = root.attrs.asdict()

    timestamps = attrs["timestamps"]

    # Now we need to update each entry in the table with the zarr index

    for i, timestamp in enumerate(timestamps):
        cur.execute("""
        INSERT INTO temp_zarr_index (harpnum, timestamp, zarr_index) VALUES (?, ?, ?)
                    """, (harpnum, timestamp, i))
    
cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_zarr_index_harpnum_timestamp ON temp_zarr_index (harpnum, timestamp)")
cur.execute("""
UPDATE HOURLY_PIXEL_BBOX
SET zarr_index = (
SELECT temp_zarr_index.zarr_index
FROM temp_zarr_index
WHERE HOURLY_PIXEL_BBOX.harpnum = temp_zarr_index.harpnum AND HOURLY_PIXEL_BBOX.timestamp = temp_zarr_index.timestamp
)
""")

conn.commit()

# Check no image is missing

cur.execute("""
SELECT COUNT(*) FROM HOURLY_PIXEL_BBOX
WHERE zarr_index IS NULL
""")

if cur.fetchone()[0] != 0:
    raise ValueError("There are missing images")

# Read SDOML image info

cur.execute("DROP TABLE IF EXISTS IMAGES")
cur.execute("""
CREATE TABLE IMAGES (
  timestamp TEXT NOT NULL UNIQUE,                 -- Unique timestamp for each image
  year INTEGER NOT NULL,                          -- Year the image was captured
  month INTEGER NOT NULL,                         -- Month the image was captured
  day INTEGER NOT NULL,                           -- Day of the month the image was captured
  hour INTEGER NOT NULL,                          -- Hour of the day the image was captured
  minute INTEGER NOT NULL,                        -- Minute of the hour the image was captured
  second INTEGER NOT NULL,                        -- Second of the minute the image was captured
  idx INTEGER NOT NULL                            -- Index to access the image in the zarr format
);
""")


with open(SDOML_TIMESTAMP_INFO, "rb") as f:
    sdoml_timestamp_info = pickle.load(f)

timestamps = list(sdoml_timestamp_info.keys())
indices = [entry["index"] for entry in sdoml_timestamp_info.values()]

i = 0
years = []
months = []
days = []
hours = []
minutes = []
seconds = []
for timestamp in timestamps:
    # Get the year, month, day, hour, minute and seconds from the timestamp
    year = int(timestamp[:4])
    month = int(timestamp[5:7])
    day = int(timestamp[8:10])
    hour = int(timestamp[11:13])
    minute = int(timestamp[14:16])
    second = int(timestamp[17:19])

    years.append(year)
    months.append(month)
    days.append(day)
    hours.append(hour)
    minutes.append(minute)
    seconds.append(second)

new_data = [(timestamp, year, month, day, hour, minute, second, idx) for timestamp, year, month, day, hour, minute, second, idx in zip(timestamps, years, months, days, hours, minutes, seconds, indices)]

cur.executemany("INSERT INTO IMAGES (timestamp, year, month, day, hour, minute, second, idx) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(timestamp) DO NOTHING", new_data)
conn.commit()

del sdoml_timestamp_info, timestamps, indices, years, months, days, hours, minutes, seconds, new_data
gc.collect()

# Select slices based on the rules:
# 1. Must have 24 images or 23 but can't be missing the last one
# 2. Harpnum must be in ACCEPTED_HARPS

cur.executescript("""
DROP TABLE IF EXISTS slice_img_counts;
DROP TABLE IF EXISTS SDOML_DATASET;
""")

cur.executescript("""
CREATE TEMP TABLE slice_img_counts AS
SELECT slice_id, COUNT(*) AS img_count, MIN( zarr_index ) AS start_index, MAX( zarr_index ) AS end_index, MIN(timestamp) AS start_image, MAX(timestamp) AS end_image
FROM GENERAL_DATASET
INNER JOIN HOURLY_PIXEL_BBOX HPB ON GENERAL_DATASET.harpnum = HPB.harpnum AND GENERAL_DATASET.obs_start <= HPB.timestamp AND GENERAL_DATASET.obs_end >= HPB.timestamp
GROUP BY slice_id;

CREATE TABLE SDOML_DATASET AS
SELECT GD.*, SIC.img_count, SIC.start_index, SIC.end_index, SIC.start_image, SIC.end_image, SIC.img_count as img_count
FROM GENERAL_DATASET GD
INNER JOIN slice_img_counts SIC ON GD.slice_id = SIC.slice_id
INNER JOIN ACCEPTED_HARPS AH ON GD.harpnum = AH.harpnum
WHERE 
SIC.img_count = 24 OR
(SIC.img_count = 23 AND ((strftime(GD.obs_end) - strftime(SIC.end_image)) / 60.0) <= 30)
""")

conn.commit()

# Now, given the real timestamp of the last image we must adjust the labels

print("Adjusting labels")

conn.executescript("""
CREATE INDEX IF NOT EXISTS idx_sdoml_dataset_cme_id ON SDOML_DATASET (cme_id);

CREATE TEMP TABLE temp_sdoml_dataset AS
SELECT slice_id, (strftime("%s", C.cme_date) - strftime("%s", SD.end_image)) / 3600.0 AS cme_diff FROM SDOML_DATASET SD
INNER JOIN CMES C
ON C.cme_id = SD.cme_id
WHERE SD.cme_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_temp_sdoml_dataset_slice_id ON temp_sdoml_dataset (slice_id);

-- Now update the cme_diff
UPDATE SDOML_DATASET
SET cme_diff = (
SELECT cme_diff FROM temp_sdoml_dataset
WHERE SDOML_DATASET.slice_id = temp_sdoml_dataset.slice_id
)
WHERE cme_id IS NOT NULL;
""")

conn.commit()

# Print some info

cur.execute("""
SELECT COUNT(DISTINCT (harpnum || cme_id)) FROM SDOML_DATASET
            """)

n_harpnum_cme_pairs = cur.fetchone()[0]

# Now with GENERAL_DATASET

cur.execute("""
SELECT COUNT(DISTINCT (harpnum || cme_id)) FROM GENERAL_DATASET
            """)

n_harpnum_cme_pairs_gd = cur.fetchone()[0]

cur.execute("""
WITH has_cme AS (
SELECT harpnum, MAX(cme_id) as cme_id FROM SDOML_DATASET GROUP BY harpnum
)
SELECT COUNT(DISTINCT harpnum) FROM has_cme
WHERE cme_id IS NOT NULL
""")

n_harpnum_with_cme = cur.fetchone()[0]

# Now with GENERAL_DATASET

cur.execute("""
WITH has_cme AS (
SELECT harpnum, MAX(cme_id) as cme_id FROM GENERAL_DATASET GROUP BY harpnum
)
SELECT COUNT(DISTINCT harpnum) FROM has_cme
WHERE cme_id IS NOT NULL
""")

n_harpnum_with_cme_gd = cur.fetchone()[0]

cur.execute("""
WITH has_cme AS (
SELECT harpnum, MAX(cme_id) as cme_id FROM SDOML_DATASET GROUP BY harpnum
)
SELECT COUNT(DISTINCT harpnum) FROM has_cme
WHERE cme_id IS NULL
""")

n_harpnum_without_cme = cur.fetchone()[0]

# Now with GENERAL_DATASET

cur.execute("""
WITH has_cme AS (
SELECT harpnum, MAX(cme_id) as cme_id FROM GENERAL_DATASET GROUP BY harpnum
)
SELECT COUNT(DISTINCT harpnum) FROM has_cme
WHERE cme_id IS NULL
""")

n_harpnum_without_cme_gd = cur.fetchone()[0]

cur.execute("""
SELECT COUNT(*) FROM SDOML_DATASET
WHERE label=1
""")

n_positive_rows = cur.fetchone()[0]

# Now with GENERAL_DATASET

cur.execute("""
SELECT COUNT(*) FROM GENERAL_DATASET
WHERE label=1
""")

n_positive_rows_gd = cur.fetchone()[0]

cur.execute("""
SELECT COUNT(*) FROM SDOML_DATASET
WHERE label=0
""")

n_negative_rows = cur.fetchone()[0]

# Now with GENERAL_DATASET

cur.execute("""
SELECT COUNT(*) FROM GENERAL_DATASET
WHERE label=0
""")

n_negative_rows_gd = cur.fetchone()[0]

print(f"""
Total number of harpnum-cme_id pairs: {n_harpnum_cme_pairs} (c.f. {n_harpnum_cme_pairs_gd})
Number of harpnums with CME: {n_harpnum_with_cme} (c.f. {n_harpnum_with_cme_gd})
Number of harpnums without CME: {n_harpnum_without_cme} (c.f. {n_harpnum_without_cme_gd})

Number of positive rows: {n_positive_rows} (c.f. {n_positive_rows_gd})
Number of negative rows: {n_negative_rows} (c.f. {n_negative_rows_gd})

Imbalance ratio: {n_negative_rows / n_positive_rows} (c.f. {n_negative_rows_gd / n_positive_rows_gd})
""")

# Now the final dataset is ready, we make some tables with only the HARPs in the
# dataset and only the CMEs in the dataset

# To keep things clear let's make a table SDOML_HARPS with only the harpnums that are in SDOML_DATASET

cur.execute("DROP TABLE IF EXISTS SDOML_HARPS")
cur.execute("DROP TABLE IF EXISTS available_harps")
cur.execute("DROP TABLE IF EXISTS SDOML_CME_HARPS_ASSOCIATIONS")
cur.execute("DROP TABLE IF EXISTS available_cmes")

cur.executescript("""
CREATE TEMP TABLE available_harps AS
SELECT DISTINCT harpnum FROM SDOML_DATASET;

CREATE TABLE SDOML_HARPS AS
SELECT H.* FROM HARPS H
INNER JOIN available_harps AH ON H.harpnum = AH.harpnum;

CREATE TEMP table available_cmes AS
SELECT DISTINCT cme_id FROM SDOML_DATASET;

CREATE TABLE SDOML_CME_HARPS_ASSOCIATIONS AS
SELECT FCHA.* FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
INNER JOIN SDOML_HARPS SH
ON FCHA.harpnum = SH.harpnum
INNER JOIN available_cmes AC
ON FCHA.cme_id = AC.cme_id;
""")

# Group overlaping HARPs together

# Now to group things for the splits

harps_df = pd.read_sql("""SELECT harpnum FROM SDOML_HARPS""", conn)
overlaps_df = pd.read_sql("""
SELECT PO.harpnum_a, PO.harpnum_b 
FROM PIXEL_OVERLAPS PO
INNER JOIN SDOML_HARPS SH1 ON  
PO.harpnum_a = SH1.harpnum
INNER JOIN SDOML_HARPS SH2 ON
PO.harpnum_b = SH2.harpnum
WHERE mean_overlap > 5 AND ocurrence_percentage > 5""", conn)

# Create an empty graph
G = nx.Graph()

# Add edges to the graph based on the overlaps table
for idx, row in overlaps_df.iterrows():
    G.add_edge(row['harpnum_a'], row['harpnum_b'])

# Find connected components
connected_components = list(nx.connected_components(G))

# Create a dictionary to store the group for each harpnum
group_dict = {}
group_size = []

used_harps = set()
for group_num, component in enumerate(connected_components):
    group_size.append((group_num, len(component)))

    for harpnum in component:
        if harpnum in used_harps:
            raise ValueError(f"The harpnum {harpnum} is in more than one group")
        used_harps.add(harpnum)
        group_dict[harpnum] = group_num
    
# Now, for all the harps that didn't get a group we keep assigning groups

group_num = len(group_dict)

for harpnum in harps_df['harpnum']:
    if harpnum not in group_dict:
        if harpnum in used_harps:
            raise ValueError(f"The harpnum {harpnum} is in more than one group")
        used_harps.add(harpnum)

        group_num += 1
        group_dict[harpnum] = group_num
        group_size.append((group_dict[harpnum], 1))

harps_group = [(group_dict[harpnum], int(harpnum)) for harpnum in group_dict.keys()]

try:
    cur.execute("ALTER TABLE SDOML_HARPS ADD COLUMN group_id INTEGER;")
except sqlite3.OperationalError as e:
    if "duplicate column name: group_id" == str(e):
        pass
    else:
        raise e

cur.executemany("""
UPDATE SDOML_HARPS
SET group_id = ?
WHERE harpnum = ?
""", harps_group)

cur.execute("DROP TABLE IF EXISTS SDOML_GROUPS")

cur.execute("""
CREATE TABLE SDOML_GROUPS (
    group_id INTEGER PRIMARY KEY,
    group_size INTEGER,
    n_level_1 INTEGER,
    n_level_2 INTEGER,
    n_level_3 INTEGER,
    n_level_4 INTEGER,
    n_level_5 INTEGER
)
""")

cur.executemany("""
INSERT INTO SDOML_GROUPS (group_id, group_size) VALUES (?, ?)
                """, group_size)

conn.commit()

# Count number of CMEs in each group

groups = pd.read_sql("""
SELECT SH.harpnum, SH.group_id, SCHA.verification_score FROM SDOML_HARPS SH
INNER JOIN SDOML_CME_HARPS_ASSOCIATIONS SCHA
ON SCHA.harpnum = SH.harpnum
""", conn)

# Count the occurrences of each verification_score within each group
count_df = groups.groupby(['group_id', 'verification_score']).size().reset_index(name='count')


# Pivot the table to have verification_scores as columns
pivot_df = count_df.pivot(index='group_id', columns='verification_score', values='count').fillna(0).reset_index()

all_group_ids_df = pd.read_sql("SELECT DISTINCT group_id FROM SDOML_GROUPS", conn)

# Merge with pivot_df, filling in missing values with zeros
final_df = pd.merge(all_group_ids_df, pivot_df, on='group_id', how='left').fillna(0)

# Generate the tuples
result_tuples = [tuple(row) for row in final_df.itertuples(index=False, name=None)]

# Need group_id to be the last element of the tuple

result_tuples = [(int(t[1]), int(t[2]), int(t[3]), int(t[4]), int(t[5]), int(t[0])) for t in result_tuples]


cur.executemany("""
UPDATE SDOML_GROUPS
SET n_level_1 = ?,
    n_level_2 = ?,
    n_level_3 = ?,
    n_level_4 = ?,
    n_level_5 = ?
WHERE group_id = ?
""", result_tuples)

conn.commit()

# Make the splits

# Now time to make the actual splits
df = pd.read_sql("""
SELECT group_id, n_level_1, n_level_2, n_level_3, n_level_4, n_level_5, group_size FROM SDOML_GROUPS
""", conn)

df.set_index('group_id', inplace=True)

# Initialize splits
N_SPLITS = 10
splits = {i: {'group_ids': [], 'totals': {col: 0 for col in df.columns if col != 'group_id'}} for i in range(1, N_SPLITS + 1)}

# Sort by each n_level_x column in descending order. We sort first level1, level2 ... until at the end group_size
df.sort_values(by=[col for col in df.columns if col != 'group_id'], ascending=False, inplace=True)

attributes = ["n_level_1", "n_level_2", "n_level_3", "n_level_4", "n_level_5", "group_size"]

# We go through the attributes in this order (order of priority)

used_groups = set()

for attribute in attributes:
    # Sort the df by the attribute

    assign_df = df[df[attribute] > 0].sort_values(by=[attribute], ascending=False)

    # Now we go row by row

    for group_id, row in assign_df.iterrows():
        if group_id in used_groups:
            raise ValueError(f"Group {group_id} already used")
        # Sort the splits to get the one with the lowest total for the attribute

        sorted_splits = sorted(splits.keys(), key=lambda x: splits[x]['totals'][attribute])

        # Get the split with the lowest total for the attribute

        split = sorted_splits[0]

        # Add the group_id to the split

        splits[split]['group_ids'].append(group_id)

        # Update the totals for the split

        for col in df.columns:
            if col != 'group_id':
                splits[split]['totals'][col] += row[col]
        
        # Take the group_id out of the df and assign_df

        df.drop(group_id, inplace=True)
        used_groups.add(group_id)

# Now, we did 10 splits because we want 5 splits, each divided into two
# So there will be a split and then a subsplit
# So we need to map every two subsplits to the same split

try:
    cur.execute("ALTER TABLE SDOML_GROUPS ADD COLUMN split INTEGER;")
except sqlite3.OperationalError as e:
    if "duplicate column name: split" == str(e):
        pass
    else:
        raise e
        
try:
    cur.execute("ALTER TABLE SDOML_GROUPS ADD COLUMN subsplit INTEGER;")
except sqlite3.OperationalError as e:
    if "duplicate column name: subsplit" == str(e):
        pass
    else:
        raise e
    
for split, split_dict in splits.items():
    group_ids = split_dict['group_ids']
    subsplit_number = split % 2 + 1
    split_number = (split + 1) // 2
    cur.executemany("UPDATE SDOML_GROUPS SET split = ?, subsplit = ? WHERE group_id = ?", [(split_number, subsplit_number, group_id) for group_id in group_ids])

conn.commit()

# Print slices info
df = pd.read_sql("""
SELECT * FROM SDOML_DATASET
INNER JOIN SDOML_HARPS ON SDOML_DATASET.harpnum = SDOML_HARPS.harpnum
INNER JOIN SDOML_GROUPS ON SDOML_HARPS.group_id = SDOML_GROUPS.group_id
""", conn)

nrows = df.groupby(['split', 'subsplit', 'label']).size().reset_index(name='counts')

n_rows_pivot = pd.pivot_table(nrows, values='counts', index=['split','subsplit'], columns=['label'], aggfunc=np.sum)

# Print in markdown
print("N Positive and negative rows")
print(n_rows_pivot.to_markdown())

# Rename the columns to neg_row and pos_row

n_rows_pivot.rename(columns={0: "neg_row", 1: "pos_row"}, inplace=True)

# Now the number of CMEs per split by verification level

ncmes = df[df["label"].astype(bool)].drop_duplicates(subset=['cme_id']).groupby(['split', 'subsplit', 'verification_level']).size().reset_index(name='counts')

ncmes_pivot = pd.pivot_table(ncmes, values='counts', index=['split', 'subsplit'], columns=['verification_level'], aggfunc=np.sum)

# To markdown
print("N cmes by verif level per split")
print(ncmes_pivot.to_markdown())

# Rename columns to n_cme_level_x

ncmes_pivot.rename(columns={1: "n_cme_level_1", 2: "n_cme_level_2", 3: "n_cme_level_3", 4: "n_cme_level_4", 5: "n_cme_level_5"}, inplace=True)

# Now number of cme productive and non cme productive regions per split

nreg = df.sort_values(by="label", ascending=False).drop_duplicates(subset=['harpnum'], keep="first").groupby(['split', 'subsplit', 'label']).size().reset_index(name='counts')

nreg_pivot = pd.pivot_table(nreg, values='counts', index=['split', 'subsplit'], columns=['label'], aggfunc=np.sum)

# To markdown

print("N regions per subsplit with and without CME")
print(nreg_pivot.to_markdown())

# Rename columns to pos_reg and neg_reg

nreg_pivot.rename(columns={0: "neg_reg", 1: "pos_reg"}, inplace=True)

# Now combine all pivots, which should share the same index
combined_pivot = pd.concat([n_rows_pivot, ncmes_pivot, nreg_pivot], axis=1)


# And save this for analysis
combined_pivot.to_csv("/home/julio/Downloads/pivots.csv")

# Print combined_pivot to markdown

print("Combined pivot")
print(combined_pivot.to_markdown())

conn.close()