fill_swan_missing_positions.py

This script is designed to process SHARPs (Spaceweather HMI Active Region Patches) data, specifically to fill in missing positions within bounding boxes over time. It identifies intervals where bounding box data is missing and interpolates these intervals using the nearest available data. The script operates in parallel to efficiently handle large datasets.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Ensures the directory for updated SHARPs data exists.
   - Clears the screen for a clean output.
   - Retrieves file paths for SHARPs data.

2. **Processing Each SHARPs Item**:
   - Reads the SHARPs data for each item.
   - Identifies rows where any of the bounding box coordinates are NaN.
   - Determines intervals where bounding box data is missing.
   - Interpolates bounding boxes for these intervals by using the nearest available data before and after the interval.
   - Updates the SHARPs data with the interpolated bounding boxes.
   - Saves the updated SHARPs data to a new file.

3. **Parallel Execution**:
   - Uses a `ProcessPoolExecutor` to process each SHARPs item in parallel, enhancing performance.
   - Displays progress using `tqdm`.

## Functions

- `get_nan_intervals(nan_bbox_mask)`:
  - Identifies intervals in the `nan_bbox_mask` where NaN values occur.
  - Parameters:
    - `nan_bbox_mask` (array-like): A mask indicating where bounding box data is NaN.
  - Yields tuples containing the start and end indices of each interval where NaN values occur.

- `process_swan_item(swan_item)`:
  - Processes a single SHARPs item to fill missing bounding box positions.
  - Parameters:
    - `swan_item` (tuple): A tuple containing the HARPNUM and the file path to the SHARPs data.
  - Reads the SHARPs data, identifies intervals where bounding box data is missing, and fills these intervals by interpolating from the nearest available data.
  - Saves the updated SHARPs data to a new file.


