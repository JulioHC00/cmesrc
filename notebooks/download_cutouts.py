import sqlite3

import zarr
import dask.array as da
import numcodecs
import numpy as np
# Import delayed
from dask import delayed

from tqdm import tqdm
import os

import s3fs

CMESRC_DB = "/home/jhc/cmesrc/cmesrc.db"
CUTOUTS_DIR = "/disk/solar15/jhc/cmesrc/cutouts/"

def get_z_groups():
    s3 = s3fs.S3FileSystem(anon=True)
    BASE_PATH = "s3://gov-nasa-hdrl-data1/contrib/fdl-sdoml/fdl-sdoml-v2/sdomlv2_hmi.zarr"
    zarrs = dict()
    print("Loading zarrs")
    for year in range(2010,2019):
        print(f"Loading zarr for year {year}")
        # 2GB cache with zarr
        store = s3fs.S3Map(root=f"{BASE_PATH}/{year}", s3=s3, check=False)
#        cached_store = zarr.LRUStoreCache(store, max_size=5e8)
        cached_store = store
        group = zarr.open_group(cached_store, mode='r')
        zarrs[year] = dict()
        zarrs[year]['Bx'] = da.from_zarr(group['Bx'])
        zarrs[year]['By'] = da.from_zarr(group['By'])
        zarrs[year]['Bz'] = da.from_zarr(group['Bz'])
        print(f"Loaded zarrs for year {year}")
    return zarrs


def get_rows_grouped_by_year(harpnum):
    con = sqlite3.connect(CMESRC_DB)
    con.execute("PRAGMA foreign_keys = ON")
    cur = con.cursor()

    cur.execute(
        f"""
        SELECT DISTINCT strftime('%Y', timestamp) FROM cutouts_for_download WHERE harpnum= ?
    """, (harpnum,))

    years = [row[0] for row in cur.fetchall()]

    rows = dict()

    for year in years:
        cur.execute("DROP TABLE IF EXISTS cutouts_for_download_temp")

        cur.execute(
            f"""
            CREATE TEMPORARY TABLE cutouts_for_download_temp AS
            SELECT hpb.timestamp, hpb.x_cen, hpb.y_cen, hpbs.width, hpbs.height, i.idx
            FROM cutouts_for_download cfd 
            INNER JOIN harps_pixel_bbox hpb ON cfd.harpnum = hpb.harpnum AND cfd.timestamp = hpb.timestamp
            INNER JOIN images i ON cfd.timestamp = i.timestamp
            INNER JOIN harps_pixel_bbox_sizes hpbs ON hpb.harpnum = hpbs.harpnum
            WHERE cfd.harpnum= ? AND strftime('%Y', cfd.timestamp) = ?
        """, (harpnum, year))

        cur.execute(
            """
            WITH cutouts_hours AS (
            SELECT *,
                strftime('%Y-%m-%d %H:00:00', timestamp) AS hour,
                ABS(julianday(timestamp) - julianday(strftime('%Y-%m-%d %H:00:00', timestamp))) * 24 * 60 * 60 AS diff
            FROM cutouts_for_download_temp
            )

            SELECT timestamp, x_cen, y_cen, width, height, idx FROM (
            SELECT *,
                RANK() OVER (PARTITION BY hour ORDER BY diff ASC) AS rank
                FROM cutouts_hours
            )
            WHERE rank = 1
            """
        )

        rows[year] = cur.fetchall()
    
    con.close()

    total_rows = sum([len(rows[year]) for year in years])

    if total_rows == 0:
        raise ValueError(f"No rows found for HARP {harpnum}")

    width, height = rows[years[0]][0][3], rows[years[0]][0][4]

    return rows, total_rows, (width, height)

def slice_image_with_padding(image, x_cen, y_cen, width, height, extra_size=10):
    # Calculate the boundaries of the slice
    width += extra_size
    height += extra_size
    x_min = x_cen - width // 2
    x_max = x_cen + width // 2
    y_min = y_cen - height // 2
    y_max = y_cen + height // 2

    # Calculate padding amounts
    pad_left = abs(min(0, x_min))
    pad_right = max(0, x_max - image.shape[1])
    pad_top = abs(min(0, y_min))
    pad_bottom = max(0, y_max - image.shape[0])

    # Pad the image
    padded_image = np.pad(image, ((pad_top, pad_bottom), (pad_left, pad_right)), 'constant', constant_values=0)

    # Adjust the slice boundaries for the padding
    x_min += pad_left
    x_max += pad_left
    y_min += pad_top
    y_max += pad_top

    # Slice the image
    slice = padded_image[y_min:y_max, x_min:x_max]

    return slice

# @delayed
def process_harps(harpnum, groups, extra_size=10):
    con = sqlite3.connect(CMESRC_DB, timeout=30)
    con.execute("PRAGMA foreign_keys = ON")
    cur = con.cursor()

    all_rows, total_rows, dims = get_rows_grouped_by_year(harpnum)

    dims = (dims[0] + extra_size, dims[1] + extra_size)

    # We also want even dimensions for simplicity

    dims = (dims[0] + dims[0] % 2, dims[1] + dims[1] % 2)

    compressor = numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.BITSHUFFLE)

    # Create zarr array
    path = f"{CUTOUTS_DIR}{harpnum}"
    os.makedirs(path, exist_ok=True)

    store = zarr.DirectoryStore(path)

    zarr_array = zarr.zeros(shape=(total_rows, 3, dims[1], dims[0]), chunks=(24, 3, dims[1], dims[0]), dtype='float32', store=store, overwrite=True, compressor=compressor)

    all_timestamps = []

#    for year in tqdm(sorted(all_rows.keys()), desc=f"Processing HARP {harpnum}"):
    for year in sorted(all_rows.keys()):
        rows = all_rows[year]
        indices = [row[-1] for row in rows]
        images_bx = groups[int(year)]['Bx'][indices]
        images_by = groups[int(year)]['By'][indices]
        images_bz = groups[int(year)]['Bz'][indices]
        da.compute(images_bx, images_by, images_bz)

#        for i, row in tqdm(enumerate(rows), total=len(rows), desc=f"Processing HARP {harpnum} for year {year}"):
        for i, row in enumerate(rows):
            timestamp, x_cen, y_cen, width, height, idx = row
            width = dims[0]
            height = dims[1]

            x_min = x_cen - width // 2
            x_max = x_cen + width // 2
            y_min = y_cen - height // 2
            y_max = y_cen + height // 2

            zarr_array[i, 0, :, :] = slice_image_with_padding(images_bx[i], x_cen, y_cen, width, height, extra_size=0)
            zarr_array[i, 1, :, :] = slice_image_with_padding(images_by[i], x_cen, y_cen, width, height, extra_size=0)
            zarr_array[i, 2, :, :] = slice_image_with_padding(images_bz[i], x_cen, y_cen, width, height, extra_size=0)

            all_timestamps.append(timestamp)
        
    # We also add metadata to the zarr array to know what timestamp each index corresponds to
    zarr_array.attrs['timestamps'] = all_timestamps

    cur.execute("INSERT INTO cutouts_for_download_processed (harpnum) VALUES (?)", (harpnum,))
    con.commit()
    con.close()

    del zarr_array
    del store
    del images_bx, images_by, images_bz 
    return

con = sqlite3.connect(CMESRC_DB)
con.execute("PRAGMA foreign_keys = ON")
cur = con.cursor()

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS cutouts_for_download_processed (
        harpnum INTEGER NOT NULL,
        PRIMARY KEY (harpnum),
        FOREIGN KEY (harpnum) REFERENCES harps (harpnum)
        )
"""
)

# Select only harps that have not been processed yet
cur.execute(
    """
    SELECT DISTINCT cfd.harpnum FROM cutouts_for_download cfd
    LEFT JOIN cutouts_for_download_processed cfdp ON cfd.harpnum = cfdp.harpnum
    WHERE cfdp.harpnum IS NULL
"""
)

harpnums_to_process = sorted([row[0] for row in cur.fetchall()])

con.close()

groups = get_z_groups()

for harpnum in tqdm(harpnums_to_process, desc="Processing harps"):
    process_harps(harpnum, groups)
