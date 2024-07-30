# temporal_matching.py

This script matches Coronal Mass Ejections (CMEs) with HARPS (HMI Active Region Patches) regions that were present on-disk at the time of the CME. It filters the LASCO CME database to include only CMEs without poor or very poor descriptions and no N points warnings.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Connects to the database containing bounding box data.
   - Reads the LASCO CME database and removes duplicates.
   - Reads the HARPS lifetime database.

2. **Filtering CMEs**:
   - Masks CMEs to include only those within the time range of available HARPS data.
   - Masks CMEs to include only those with good quality flags.

3. **Finding Matching Regions**:
   - Sorts the start and end times of HARPS lifetimes.
   - Iterates through each CME time to find matching HARPS regions that were active at that time.
   - Constructs a new database with matched CMEs and HARPS regions.

4. **Saving Results**:
   - Saves the matched CMEs and HARPS regions to a CSV file and a pickle file.

## Functions

- `findAllMatchingRegions()`:
  - Finds all HARPS regions that match each CME temporally.
  - Sorts the start and end times of HARPS lifetimes.
  - Iterates through each CME time to find matching HARPS regions.
  - Constructs a new database with matched CMEs and HARPS regions.
  - Saves the matched CMEs and HARPS regions to a CSV file and a pickle file.
