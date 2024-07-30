"""
This script fills missing positions in SWAN data by interpolating bounding boxes
for intervals where data is missing. It processes each SWAN item in parallel using
a ProcessPoolExecutor.
"""

from src.cmesrc.utils import filepaths_dt_swan_data, clear_screen, read_SWAN_filepath
from src.cmesrc.config import UPDATED_SWAN
from tqdm import tqdm
from src.harps.harps import Harps
import pandas as pd
import numexpr as ne
import numpy as np
from os.path import join, exists
from os import mkdir
import concurrent.futures

# Ensure the directory for updated SWAN data exists
if not exists(UPDATED_SWAN):
    mkdir(UPDATED_SWAN)

clear_screen()

# Get file paths for SWAN data
SWAN = filepaths_dt_swan_data()

clear_screen()

print("== FILLING MISSING VALUES ==")


def get_nan_intervals(nan_bbox_mask):
    """
    Identify intervals in the nan_bbox_mask where NaN values occur.

    Parameters:
    nan_bbox_mask (array-like): A mask indicating where bounding box data is NaN.

    Yields:
    tuple: A tuple containing the start and end indices of each interval where NaN values occur.
    """
    start = None
    end = None

    for i, value in enumerate(nan_bbox_mask):
        if value and start is None:
            start = i

        elif not value and start is not None:
            end = i

            yield start, end

            start = None
            end = None

        if i == len(nan_bbox_mask) - 1 and start is not None:
            end = i + 1

            yield start, end

            start = None
            end = None


def process_swan_item(swan_item):
    """
    Process a single SWAN item to fill missing bounding box positions.

    Parameters:
    swan_item (tuple): A tuple containing the HARPNUM and the file path to the SWAN data.

    This function reads the SWAN data, identifies intervals where bounding box data is missing,
    and fills these intervals by interpolating from the nearest available data. The updated
    SWAN data is then saved to a new file.
    """
    harpnum, swan_filepath = swan_item
    swan_harp = read_SWAN_filepath(swan_filepath)

    # Identify rows where any of the bounding box coordinates are NaN
    nan_bbox_mask = (
        swan_harp[["LONDTMIN", "LONDTMAX", "LATDTMIN", "LATDTMAX"]].isna().any(axis=1)
    )

    new_swan_harp = swan_harp.copy()
    new_swan_harp["IRBB"] = False

    intervals = list(get_nan_intervals(nan_bbox_mask))

    for interval in intervals:
        start, end = interval
        middle = (start + end) // 2

        # Determine the indices for the nearest available data before and after the interval
        if start != 0:
            first_index = start - 1
        else:
            first_index = end

        if end != len(new_swan_harp):
            last_index = end
        else:
            last_index = start - 1

        first_harp_data = swan_harp.iloc[first_index][
            ["Timestamp", "LONDTMIN", "LATDTMIN", "LONDTMAX", "LATDTMAX"]
        ].to_list()
        last_harp_data = swan_harp.iloc[last_index][
            ["Timestamp", "LONDTMIN", "LATDTMIN", "LONDTMAX", "LATDTMAX"]
        ].to_list()

        first_harps = Harps(*first_harp_data)
        last_harps = Harps(*last_harp_data)

        # Interpolate bounding boxes for the interval
        harps = [first_harps if i <= middle else last_harps for i in range(start, end)]

        incomplete_rows = swan_harp.iloc[start:end]
        incomplete_indices = incomplete_rows.index

        new_timestamps = incomplete_rows["Timestamp"].to_numpy()
        new_bboxes = np.array(
            [
                harps[i].rotate_bbox(new_timestamps[i], keep_shape=True).get_raw_bbox()
                for i in range(len(harps))
            ]
        )

        # Update the new SWAN data with the interpolated bounding boxes
        new_swan_harp.loc[incomplete_indices, "LONDTMIN"] = new_bboxes[:, 0, 0]
        new_swan_harp.loc[incomplete_indices, "LATDTMIN"] = new_bboxes[:, 0, 1]
        new_swan_harp.loc[incomplete_indices, "LONDTMAX"] = new_bboxes[:, 1, 0]
        new_swan_harp.loc[incomplete_indices, "LATDTMAX"] = new_bboxes[:, 1, 1]

        new_swan_harp.loc[incomplete_indices, "IRBB"] = True

    filename = f"{harpnum}.csv"

    # Save the updated SWAN data to a new file
    new_swan_harp.to_csv(join(UPDATED_SWAN, filename), sep="\t", index=False)


num_threads = 4

with concurrent.futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
    futures = [
        executor.submit(process_swan_item, swan_item) for swan_item in SWAN.items()
    ]

    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        pass

