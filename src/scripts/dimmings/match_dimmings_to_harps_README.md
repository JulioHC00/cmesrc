# match_dimmings_to_harps.py

This script matches dimmings to HARPS (HMI Active Region Patches) regions based on temporal and spatial criteria. It ensures that the HARPS regions were present on-disk at the time of the dimming and performs spatial matching based on the position angles and distances from the Sun's center.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Connects to the database containing bounding box data.
   - Reads the dimmings catalogue.
   - Sets up necessary indices and data structures for efficient processing.

2. **Finding HARPS Regions Present at Dimming Time**:
   - For each dimming, finds the HARPS regions that were present on-disk at the time of the dimming.
   - Uses binary search to efficiently find the HARPS regions.

3. **Calculating Distances**:
   - For each dimming, calculates the distance to the closest HARPS region.
   - Uses spherical geometry to compute the distances.

4. **Scoring and Matching**:
   - Assigns scores based on the distances.
   - Matches each dimming to the HARPS region with the highest score.
   - Ensures no duplicate matches are made.

5. **Saving Results**:
   - Saves the matched dimmings and HARPS regions to a final database.
   - Saves the final database to CSV and pickle files.

## Functions

- `gather_dimming_distances()`:
  - Main function that orchestrates the matching process.
  - Reads the dimmings catalogue and HARPS lifetime database.
  - Finds HARPS regions present at the time of each dimming.
  - Calculates distances and scores.
  - Matches dimmings to HARPS regions.
  - Saves the results to files.
