from src.cmesrc.config import CMESRC_DB, HARPS_LIFETIME_DATABSE, SDOML_TIMESTAMP_INFO
from src.cmesrc.utils import read_SWAN_filepath, filepaths_updated_swan_data
from tqdm import tqdm
import sqlite3
import pandas as pd
import pickle

with open(SDOML_TIMESTAMP_INFO, "rb") as f:
    sdoml_timestamp_info = pickle.load(f)

timestamps = list(sdoml_timestamp_info.keys())
indices = [entry["index"] for entry in sdoml_timestamp_info.values()]

con = sqlite3.connect(CMESRC_DB)
con.execute("PRAGMA foreign_keys = ON")
cur = con.cursor()

# First we add the timestamps of the images

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

# WILL NOT OVERWRITE

cur.executemany("INSERT INTO images (timestamp, year, month, day, hour, minute, second, idx) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(timestamp) DO NOTHING", new_data)
con.commit()

# Now we add the harps lifetimes

df = pd.read_csv(HARPS_LIFETIME_DATABSE)
df.rename(
    columns={
        "harpsnum": "harpnum"
        }, 
    inplace=True
    )

df["harpnum"] = df["harpnum"].astype(int)
df["start"] = df["start"].astype(str).apply(lambda x: x[:-4])
df["end"] = df["end"].astype(str).apply(lambda x: x[:-4])

new_data = df.values

for data in new_data:
    try:
        cur.execute("INSERT INTO harps (harpnum, start, end) VALUES (?, ?, ?) ON CONFLICT(harpnum) DO NOTHING", data)
    except sqlite3.IntegrityError as e:
        # Sometimes the timestamp doesn't exist in the images so we find the closest
        cur.execute("SELECT COUNT(*) FROM images WHERE timestamp = ?", (data[1],))
        start_exists = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM images WHERE timestamp = ?", (data[2],))
        end_exists = cur.fetchone()[0]

        closest_start = data[1]
        closest_end = data[2]

        if not start_exists:
            cur.execute("SELECT timestamp FROM images WHERE timestamp >= ? ORDER BY timestamp ASC LIMIT 1;", (closest_start,))
            closest_start = cur.fetchone()[0]
        if not end_exists:
            cur.execute("SELECT timestamp FROM images WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1;", (closest_end,))
            closest_end = cur.fetchone()[0]

        cur.execute("INSERT INTO harps (harpnum, start, end) VALUES (?, ?, ?) ON CONFLICT(harpnum) DO NOTHING", (int(data[0]), closest_start, closest_end))

con.commit()
con.close()