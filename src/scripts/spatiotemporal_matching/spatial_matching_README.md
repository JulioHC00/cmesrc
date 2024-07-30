# spatial_matching.py

This script matches temporally co-occurring HARPS (HMI Active Region Patches) regions to Coronal Mass Ejections (CMEs). It ensures that the HARPS regions were present on-disk at the time of the CME and performs spatial matching based on the position angles and distances from the Sun's center.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Connects to the database containing bounding box data.
   - Reads the temporally matched HARPS and CME data.
   - Sets up necessary indices and data structures for efficient processing.

2. **Finding Closest HARPS Positions**:
   - For each CME, finds the closest HARPS positions in time.
   - Rotates the HARPS bounding boxes to the CME detection time if necessary.

3. **Spatial Matching**:
   - Calculates the position angles and distances from the Sun's center for both CMEs and HARPS.
   - Determines if the HARPS regions are within the CME's width and position angle range.
   - Saves the matched CMEs and HARPS regions to a final database.

4. **Parallel Execution**:
   - Uses multiprocessing to process each CME in parallel, enhancing performance.

## Functions

- `setup()`:
  - Initializes the database connection and reads the temporally matched HARPS and CME data.
  - Sets up necessary indices and data structures for efficient processing.
  - Returns the final database with HARPS raw coordinates and dates.

- `findSpatialCoOcurrentHarps(cme_ids)`:
  - Finds spatially co-occurring HARPS regions for the given CME IDs.
  - Rotates the HARPS bounding boxes to the CME detection time if necessary.
  - Calculates the position angles and distances from the Sun's center for both CMEs and HARPS.
  - Returns the final database with HARPS coordinates and position angles.

- `find_matches_and_save(final_database)`:
  - Determines if the HARPS regions are within the CME's width and position angle range.
  - Saves the matched CMEs and HARPS regions to a final database.
  - Saves the final database to CSV and pickle files.
