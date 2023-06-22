import sqlite3
import boto3
import os

import zarr
import xarray as xr
import dask.array as da
import numcodecs
import numpy as np
import fsspec
# Import delayed
from dask import delayed

from tqdm import tqdm
import os

import s3fs

from dask.diagnostics import ProgressBar
from dask.distributed import Client, progress
from collections import defaultdict
import dask
import logging

logging.basicConfig(level=logging.INFO, filename='cutouts.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

from dask.distributed import Client, as_completed


#CMESRC_DB = "/home/julio/cmesrc/data/processed/cmesrc.db"
CMESRC_DB = "/home/jhc/cmesrc/cmesrc.db"
#CUTOUTS_DIR = "/home/julio/cmesrc/data/processed/images"
CUTOUTS_DIR = "/disk/solar15/jhc/cmesrc/cutouts"
CACHE_DIR = "/disk/solar15/jhc/cmesrc/cache"
CACHE_SIZE = 1e9

# Initialize a S3 file system
fs = s3fs.S3FileSystem(anon=True)
store = s3fs.S3Map(root='s3://gov-nasa-hdrl-data1/contrib/fdl-sdoml/fdl-sdoml-v2/sdomlv2_hmi.zarr', s3=fs)
cache_store = store # Tried to use cache but couldn't control its size

# Define channels
channels = ['Bx', 'By', 'Bz']


def process_harpnum(harpnum):

    # Get the rows grouped by year
    rows_by_year, total_rows, shape = get_rows_grouped_by_year(harpnum)

    # Make sure shape is divisible by 2

    shape = (shape[0] + shape[0] % 2, shape[1] + shape[1] % 2)

    width, height = shape

    compressor = numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.BITSHUFFLE)

    # Create a Zarr group to store all cutouts for this harpnum
    root_path = f'{CUTOUTS_DIR}/{harpnum}'
    root_store = zarr.DirectoryStore(root_path)
    root_group = zarr.group(store=root_store, overwrite=True)

    EXTRA_SIZE = 40

    width += EXTRA_SIZE
    height += EXTRA_SIZE

    all_timestamps = []

    cutouts_dict = defaultdict(list)
    
    for year, rows in rows_by_year.items():
        
        for channel in channels:
            # Load the year and channel specific zarr group into a Dask array
            dask_arr = da.from_zarr(cache_store, component=f'{year}/{channel}')
            
            for row in rows:
                timestamp, x_cen, y_cen, n_width, n_height, idx = row

                # Add the timestamp to the list of timestamps.
                # We check the channel so we only add the timestamp once
                if channel == 'Bz':
                    all_timestamps.append(timestamp)

                # Fetch the required image from the Dask array (this won't load the data yet)
                image = dask_arr[idx]

                # Slice the image with padding (this won't load the data yet)
                cutout = dask.delayed(slice_image_with_padding)(image, x_cen, y_cen, width, height, extra_size=0)

                # Convert each delayed cutout into a dask array
                cutout_da = da.from_delayed(cutout, shape=(height, width), dtype=np.float32)
                
                # Store the cutout for future computation
                cutouts_dict[channel].append(cutout_da)

    stacked_cutouts = []
    # Create the final zarr array for each year
    for channel, cutouts in cutouts_dict.items():
        # Stack cutouts (this won't load the data yet)
        stacked_cutouts.append(da.stack(cutouts))

    # Stack all channels
    stacked_channels = da.stack(stacked_cutouts, axis=1)

    # Rechunk to shape (n_cutouts, 3, height, width)
    stacked_channels = stacked_channels.rechunk((48, 3, height, width))

    # Check correct lengths
    if len(stacked_channels) != total_rows:
        # Log which harpnum has a problem, describing the problem
        logging.error(f"Problem with harpnum {harpnum} and year {year}. Expected {total_rows} cutouts, got {len(stacked_channels)}")


    metadata = {'timestamps': all_timestamps}

    # Store the data to zarr group and add all_timestamps as metadata
    stacked_channels.to_zarr(url=root_path, overwrite=True, compressor=compressor)

    # Add metadata

    root_group.attrs['timestamps'] = all_timestamps

    with sqlite3.connect(CMESRC_DB, timeout=30) as con:
        con.execute("PRAGMA foreign_keys = ON")
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO cutouts_for_download_processed (harpnum) VALUES (?)", (harpnum,))
        con.commit()

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

if __name__ == "__main__":
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

    # Process each harpnum in the list
    for harpnum in tqdm(harpnums_to_process):
        process_harpnum(harpnum)