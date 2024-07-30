# match_flares_to_harps.py

This script matches flares to HARPS (HMI Active Region Patches) regions based on temporal and spatial criteria. It ensures that the HARPS regions were present on-disk at the time of the flare and performs spatial matching based on the position angles and distances from the Sun's center.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Connects to the database containing bounding box data.
   - Reads the flares catalogue.
   - Sets up necessary indices and data structures for efficient processing.

2. **Finding HARPS Regions Present at Flare Time**:
   - For each flare, finds the HARPS regions that were present on-disk at the time of the flare.
   - Uses binary search to efficiently find the HARPS regions.

3. **Calculating Distances**:
   - For each flare, calculates the distance to the closest HARPS region.
   - Uses spherical geometry to compute the distances.

4. **Scoring and Matching**:
   - Assigns scores based on the distances.
   - Matches each flare to the HARPS region with the highest score.
   - Ensures no duplicate matches are made.

5. **Saving Results**:
   - Saves the matched flares and HARPS regions to a final database.
   - Saves the final database to CSV and pickle files.

## Functions

- `flare_class_to_number(fclass)`:
  - Converts the flare class (e.g., 'A', 'B', 'C', 'M', 'X') to a numerical score.
  - Parameters:
    - `fclass` (str): The flare class string.
  - Returns the numerical score corresponding to the flare class.
