parse_lasco_cme_catalogue.py

This script is designed to parse the raw text file of the LASCO CME (Coronal Mass Ejection) catalogue into a CSV format. It processes the raw data to generate unique IDs and categorizes various attributes of the CMEs.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Opens the raw LASCO CME catalogue text file.
   - Reads all lines from the file, excluding the header.

2. **Data Separation**:
   - Separates comments from the main data lines.
   - Prepares a Pandas DataFrame to hold the processed data.

3. **Data Parsing**:
   - Iterates through each data line and comment pair.
   - Cleans and processes each column of data:
     - **Removes missing values represented by "----":** Any data column that contains "----" is considered missing and is replaced with an empty string.
     - **Removes non-reliable measurements marked with "*":** Measurements that are marked with a "*" are considered non-reliable due to large uncertainties or other issues. These are also replaced with an empty string.
     - **Handles Halo CMEs:** For CMEs classified as Halo, the width column contains the word "Halo". This word is removed, and the width is set to an empty string. Additionally, a halo flag is set to 1 to indicate that the CME is a Halo event.
     - **Extracts flags from comments:** Comments are parsed to extract additional attributes:
       - **seen_in:** Indicates in which LASCO instrument the CME was seen. It is set to 0 for both C2 and C3, 1 for C2 only, and 2 for C3 only.
       - **quality:** Indicates the quality of the event. It is set to 0 for no comments, 1 for Poor Event, and 2 for Very Poor Event.
       - **three_points:** Indicates the number of points used to define the CME. It is set to the number of points if specified in the comment, otherwise, it is set to 0.

4. **ID Generation**:
   - **Generates a unique ID for each CME entry:** A unique ID is generated for each CME entry by combining the date (in the format YYYYMMDD) and the principal angle (PA). If the PA is missing, it is replaced with "999" to ensure uniqueness. The ID format is "IDYYYYMMDDPA", where PA is zero-padded to three digits.

5. **Saving Processed Data**:
   - **Constructs a dictionary for each row of processed data:** Each row of processed data is represented as a dictionary where the keys are the column names and the values are the processed data for that row.
   - **Appends each dictionary to a list:** All dictionaries (rows) are appended to a list, which will be used to create the final DataFrame.
   - **Converts the list of dictionaries into a Pandas DataFrame:** The list of dictionaries is converted into a Pandas DataFrame, which is a tabular data structure.
   - **Saves the DataFrame to a CSV file:** The DataFrame is saved to a CSV file, which is the final output format of the script. This file can be easily read and analyzed using various tools and libraries.

## Functions

- `parse_lasco_cme_catalogue()`:
  - Main function that orchestrates the parsing of the LASCO CME catalogue.
  - Opens the raw text file, reads lines, separates comments, and processes each data line.
  - Cleans and processes each column of data, generates unique IDs, and constructs rows for the DataFrame.
  - Saves the processed data to a CSV file.
