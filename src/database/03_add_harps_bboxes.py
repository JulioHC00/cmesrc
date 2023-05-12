from src.cmesrc.config import CMESRC_DB, HARPS_LIFETIME_DATABSE, SDOML_TIMESTAMP_INFO
from src.cmesrc.utils import read_SWAN_filepath, filepaths_updated_swan_data
from tqdm import tqdm
import sqlite3
import pandas as pd
import pickle
from astropy.time import Time

con = sqlite3.connect(CMESRC_DB)
con.execute("PRAGMA foreign_keys = ON")
cur = con.cursor()

SWAN = filepaths_updated_swan_data()
for harpnum, filepath in tqdm(SWAN.items()):
    data = read_SWAN_filepath(filepath)
    cur.execute("SELECT START, END FROM harps WHERE HARPNUM = ?", (int(harpnum),))
    start, end = cur.fetchone()

    start = Time(start)
    end = Time(end)

    data = data[(data["Timestamp"] >= start) & (data["Timestamp"] <= end)]
    df_data = data[["Timestamp", "LONDTMIN", "LONDTMAX", "LATDTMIN", "LATDTMAX", "IRBB"]].values
    new_data = [(int(harpnum), str(timestamp.to_value("iso")[:-4]), float(lonmin), float(lonmax), float(latmin), float(latmax), int(irbb)) for timestamp, lonmin, lonmax, latmin, latmax, irbb in df_data]

    for data in new_data:
        try:
            cur.execute("INSERT INTO harps_bbox (harpnum, timestamp, LONDTMIN, LONDTMAX, LATDTMIN, LATDTMAX, IRBB) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(harpnum, timestamp) DO NOTHING", data)
        except sqlite3.IntegrityError as e:
            cur.execute("SELECT COUNT(*) FROM images WHERE timestamp = ?", (data[1],))
            timestamp_exists = cur.fetchone()[0]

            # Where a timestamp doesn't exist in the images, we just don't add it
            if "FOREIGN KEY" in e.args[0] and not timestamp_exists:
                continue
            else:
                raise e
con.commit()
con.close()