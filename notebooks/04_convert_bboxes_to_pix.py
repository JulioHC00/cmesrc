from astropy.coordinates import SkyCoord
from sunpy.coordinates import HeliographicStonyhurst
import astropy.units as u
from tqdm import tqdm
import sunpy.map as smap
import logging
import multiprocessing as mp
import sqlite3
import numpy as np
from queue import Empty as QueueEmpty
import pickle
import time

SDOML_TIMESTAMP_INFO = "timestamp_info.pkl"
CMESRC_DB = "cmesrc.db"

#########################################################
#########################################################
### THIS SCRIPT NEEDS BREAKING THE DB INTO PARTITIONS ###
#########################################################
#########################################################

logging.basicConfig(level=logging.INFO, filename="pixel_commit_process.log", filemode="w")

with open(SDOML_TIMESTAMP_INFO, "rb") as f:
    sdoml_timestamp_info = pickle.load(f)


con = sqlite3.connect(CMESRC_DB, timeout=1)
con.execute("PRAGMA foreign_keys = ON")
cur = con.cursor()

cur.execute("SELECT COUNT(*) FROM harps_bbox LIMIT 1;")

no_join_length = cur.fetchone()[0]

cur.execute("""
SELECT COUNT(*) FROM harps_bbox hb
LEFT OUTER JOIN harps_pixel_bbox hpb
ON hb.harpnum = hpb.harpnum AND hb.timestamp = hpb.timestamp
WHERE hpb.harpnum IS NULL AND hpb.timestamp IS NULL LIMIT 1;""")
total_length = cur.fetchone()[0]

con.close()


def calculate_pixel_values(offset, length, pid, queue):
    pbar = tqdm(total=length)

    con = sqlite3.connect("cmesrc.db", timeout=1)
    con.execute("PRAGMA foreign_keys = ON")

    cursor = con.cursor()

    cursor.execute(f"""
    SELECT hb.harpnum, hb.timestamp, hb.londtmin, hb.londtmax, hb.latdtmin, hb.latdtmax
    FROM harps_bbox hb
    LEFT OUTER JOIN harps_pixel_bbox hpb
    ON hb.harpnum = hpb.harpnum AND hb.timestamp = hpb.timestamp
    WHERE hpb.harpnum IS NULL AND hpb.timestamp IS NULL
    ORDER BY hb.harpnum, hb.timestamp LIMIT {length} OFFSET {offset};""")

#    row = cursor.fetchone()
    rows = cursor.fetchall()

    con.close()


    data = np.zeros((512, 512))

    i = 0

    try:
        with tqdm(total=length, position = pid+1) as pbar:
            #while row is not None:
            while len(rows) > 0:
                row = rows.pop(0)

                harpnum, timestamp, londtmin, londtmax, latdtmin, latdtmax = row

                mean_lon = (londtmin + londtmax) / 2
                mean_lat = (latdtmin + latdtmax) / 2

                header = sdoml_timestamp_info[timestamp]["header"]

                sunpy_map = smap.Map(data, header)

                bottom_left = SkyCoord(londtmin, latdtmin, unit="deg", frame=HeliographicStonyhurst)
                upper_right = SkyCoord(londtmax, latdtmax, unit="deg", frame=HeliographicStonyhurst)
                top_left = SkyCoord(londtmin, latdtmax, unit="deg", frame=HeliographicStonyhurst)
                bottom_right = SkyCoord(londtmax, latdtmin, unit="deg", frame=HeliographicStonyhurst)

                if (mean_lon >= 0 and mean_lat <= 0) or (mean_lon <= 0 and mean_lat >= 0):
                    bottom_left_pix = sunpy_map.world_to_pixel(bottom_left)
                    upper_right_pix = sunpy_map.world_to_pixel(upper_right)

                    x_min = int(bottom_left_pix[0].value)
                    y_min = int(bottom_left_pix[1].value)
                    x_max = int(upper_right_pix[0].value)
                    y_max = int(upper_right_pix[1].value)
                else:
                    top_left_pix = sunpy_map.world_to_pixel(top_left)
                    bottom_right_pix = sunpy_map.world_to_pixel(bottom_right)

                    x_min = int(top_left_pix[0].value)
                    y_min = int(bottom_right_pix[1].value)
                    x_max = int(bottom_right_pix[0].value)
                    y_max = int(top_left_pix[1].value)

                x_cent = int((x_min + x_max) / 2)
                y_cent = int((y_min + y_max) / 2)

                new_row = (str(timestamp), int(harpnum), x_min, x_max, y_min, y_max, x_cent, y_cent)

                queue.put(new_row)


#                row = cursor.fetchone()
                pbar.update(1)
                
                i += 1

    except KeyboardInterrupt:
        print("Interrupted")
        print(f"{i} out of {length}")
        con.commit()
        con.close()
        pbar.close()

def insert_rows_from_queue(queue):
    logging.info(f"Starting insert process")

    con = sqlite3.connect("cmesrc.db", timeout=10)
    con.execute("PRAGMA foreign_keys = ON")
    
    write_cursor = con.cursor()

    inserted_rows = 0
    rows_since_last_commit = 0
    while True:
        try:
            rows = queue.get(timeout=10)
            
            if not isinstance(rows, list):
                rows = [rows]

            write_cursor.executemany("INSERT INTO harps_pixel_bbox (timestamp, harpnum, x_min, x_max, y_min, y_max, x_cen, y_cen) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (harpnum, timestamp) DO NOTHING;", rows)

            inserted_rows += len(rows) 
            rows_since_last_commit += len(rows)

            if rows_since_last_commit > 10000:
                logging.info(f"Inserted {inserted_rows} out of {total_length} rows ({queue.qsize()} in queue)")
                con.commit()
                rows_since_last_commit = 0

        except QueueEmpty:
            logging.info(f"Writting proccess finished. Inserted {inserted_rows} out of {PARTITION_LEN} rows ({queue.qsize()} in queue)")
            con.commit()
            con.close()
            break
        except KeyboardInterrupt:
            print("Interrupted")
            print(f"{inserted_rows} out of {total_length}")
            con.commit()
            con.close()
            break
        except Exception as e:
            logging.error(f"Error in write process: {e}")
            con.commit()
            con.close()
            raise e

if __name__ == "__main__":

        PARTITION_LEN = 200000

        PARTITIONS = total_length // PARTITION_LEN + 1

        LAST_PARTITION_LEN = total_length % PARTITION_LEN

        N_THREADS = 8

        queue = mp.Manager().Queue(maxsize=20000)

        for partition in range(PARTITIONS):
            start = time.time()
            if partition == PARTITIONS - 1:
                # calculate length of final thread
                part_length = LAST_PARTITION_LEN
            else:
                # calculate length of other threads
                part_length = PARTITION_LEN

            logging.info(f"Starting partition {partition}/{PARTITIONS-1}")

            EXTRA_OFFSET = partition * PARTITION_LEN

            processes = []

            write_process = mp.Process(target=insert_rows_from_queue, args=(queue,))
            processes.append(write_process)
            write_process.start()

            for i in range(N_THREADS):
                if i == N_THREADS - 1:
                    # calculate length of final thread
                    length = part_length // N_THREADS + part_length % N_THREADS
                else:
                    # calculate length of other threads
                    length = part_length // N_THREADS
                
                offset = i * (part_length // N_THREADS) + EXTRA_OFFSET

                p = mp.Process(target=calculate_pixel_values, args=(offset, length, i, queue))
                processes.append(p)
                p.start()

            for process in processes:
                process.join()

            logging.info(f"Finished partition {partition}/{PARTITIONS-1} in {time.time() - start} seconds")