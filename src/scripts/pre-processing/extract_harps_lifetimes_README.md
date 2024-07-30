# extract_harps_lifetimes.py

This script is designed to process SHARPs (Spaceweather HMI Active Region Patches) data to extract a list of all HARPs (HMI Active Region Patches) regions along with the time of their first appearance in HARPs data and last appearance. This information is crucial for tracking the lifecycle of active regions.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Ensures the directory for updated SHARPs data exists.
   - Retrieves file paths for SHARPs data.

2. **Processing Each SHARPs Item**:
   - Reads the SHARPs data for each item.
   - Extracts the first and last timestamp for each HARPs region.
   - Stores this information in a dictionary.

3. **Generating the HARPs Lifetime Database**:
   - Converts the dictionary into a DataFrame.
   - Saves the DataFrame to a CSV file.

## Functions

- `generate_HARPS_lifetime_database()`:
  - Processes the SHARPs data to extract the first and last timestamp for each HARPs region.
  - Parameters: None
  - Reads the SHARPs data, extracts the timestamps, and stores them in a dictionary.
  - Converts the dictionary into a DataFrame and saves it to a CSV file.
