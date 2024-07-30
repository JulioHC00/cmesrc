# match_flares_to_harps.py

This script processes flare data from SWAN (Solar Wind ANisotropies) files and matches flares to HARPS (HMI Active Region Patches) regions based on their occurrence time and spatial attributes.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Reads the SWAN files containing flare data.
   - Sets up necessary data structures for efficient processing.

2. **Extracting Flares from SWAN Data**:
   - For each HARPS region in the SWAN data, identifies rows where flares are recorded.
   - Extracts flare details including timestamp, location, and class.

3. **Processing Flares**:
   - Converts flare class to a numerical score using the `flare_class_to_number` function.
   - Collects flare data into a structured format.

4. **Saving Results**:
   - Saves the processed flare data to a CSV file and a pickle file.
   - Ensures no duplicate flare entries are saved.

5. **Saving Results**:
   - Saves the matched flares and HARPS regions to a final database.
   - Saves the final database to CSV and pickle files.

## Functions

- `flare_class_to_number(fclass)`:
  - Converts the flare class (e.g., 'A', 'B', 'C', 'M', 'X') to a numerical score for easier comparison and processing.
  - Parameters:
    - `fclass` (str): The flare class string.
  - Returns the numerical score corresponding to the flare class.
