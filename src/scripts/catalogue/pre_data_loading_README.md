# Pre-Data Loading Script Documentation

## Overview

This document explains the process of creating processed bounding boxes from raw bounding boxes as implemented in the `pre_data_loading.py` script. The script follows a series of steps to ensure the data is cleaned, validated, and transformed for further analysis.

## Steps to Create Processed Bounding Boxes

### 1. Data Preparation

- **Database Initialization**: The script starts by initializing the SQLite database and creating necessary tables for HARPS, CMEs, Flares, and other related entities.
- **Data Loading**: It then loads the raw bounding box data from SWAN files into the `RAW_HARPS_BBOX` table.

### 2. Data Cleaning

- **Remove Invalid Bounding Boxes**: The script removes bounding boxes that are invalid or extend beyond the solar limb.
- **Trim Bounding Boxes**: It trims the bounding boxes to ensure they do not exceed the -90 to 90 degree range in longitude.

### 3. Calculating Areas and Overlaps

- **Calculate Area**: The script calculates the area of each HARP region based on the bounding box coordinates.
- **Calculate Overlaps**: It computes the overlap percentage between different HARP regions to identify potential duplicates.

### 4. Handling Overlaps

- **Identify and Resolve Overlaps**: The script identifies HARP regions that significantly overlap and decides whether to merge or delete one of the regions based on the overlap percentage and occurrence.
- **Record Decisions**: It records the decisions made for each overlap in the `OVERLAP_RECORDS` table.

### 5. Creating Processed Bounding Boxes

- **Filter Out Problematic Regions**: The script filters out regions that were marked for deletion or merging during the overlap resolution process.
- **Finalize Processed Bounding Boxes**: It creates the `PROCESSED_HARPS_BBOX` table containing the cleaned and validated bounding boxes ready for further analysis.

## Conclusion

The `pre_data_loading.py` script ensures that the raw bounding box data is transformed into a clean, validated, and ready-to-use format for subsequent analyses. Each step is designed to maintain data integrity and accuracy, ensuring reliable results in downstream applications.
