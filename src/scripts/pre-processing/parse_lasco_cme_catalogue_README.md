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
     - Removes missing values represented by "----".
     - Removes non-reliable measurements marked with "*".
     - Handles Halo CMEs by setting the width to an empty string and adding a halo flag.
     - Extracts flags from comments for seen_in, quality, and three_points attributes.

4. **ID Generation**:
   - Generates a unique ID for each CME entry by combining the date and principal angle.

5. **Saving Processed Data**:
   - Constructs a dictionary for each row of processed data.
   - Appends each dictionary to a list.
   - Converts the list of dictionaries into a Pandas DataFrame.
   - Saves the DataFrame to a CSV file.

## Functions

- `parse_lasco_cme_catalogue()`:
  - Main function that orchestrates the parsing of the LASCO CME catalogue.
  - Opens the raw text file, reads lines, separates comments, and processes each data line.
  - Cleans and processes each column of data, generates unique IDs, and constructs rows for the DataFrame.
  - Saves the processed data to a CSV file.
