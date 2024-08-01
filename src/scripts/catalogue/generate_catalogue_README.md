# generate_catalogue.py

This script is designed to generate a comprehensive catalogue of solar events by processing various data sources, including LASCO CME (Coronal Mass Ejection) catalogue, SHARPs (Spaceweather HMI Active Region Patches) data, and other associated events like flares and dimmings. The script ensures that the data is loaded into a SQLite database and associated correctly based on temporal and spatial criteria.

## Overview

The script performs the following steps:

1. **Initialization and Setup**:
   - Ensures the directory for the database exists.
   - Clears the screen for a clean output.
   - Copies the bounding box data to the database.

2. **Loading Data into Database**:
   - Reads the LASCO CME catalogue and loads it into the database.
   - Processes spatially consistent CME-HARP associations and loads them into the database.
   - Reads and loads dimming and flare data into the database.
   - Calculates the closest timestamps for dimmings, flares, and CMEs.

3. **Associating Events**:
   - **Temporal Association**:
     - For each CME, the script identifies the closest flares and dimmings based on their timestamps.
     - The closest events are determined within a specified temporal threshold.
   - **Spatial Association**:
     - The script verifies if the identified flares and dimmings are spatially consistent with the CME based on their coordinates.
   - **Verification Level Determination**:
     - The verification score is determined based on the presence of both flares and dimmings, and the intensity of the flares.
     - Higher verification scores are assigned if both flares and dimmings are present and the flare intensity exceeds a predefined threshold.
   - **Saving Associations**:
     - The associations between CMEs, flares, and dimmings, along with their verification scores, are saved into the database for further analysis.

## Functions

- `clear_screen()`:
  - Clears the screen for a clean output.
  - Parameters: None

- `closest_timestamp(target, sorted_timestamps)`:
  - Finds the closest timestamp to the target from a list of sorted timestamps.
  - Parameters:
    - `target` (datetime): The target timestamp.
    - `sorted_timestamps` (list): A list of sorted datetime objects.
  - Returns the closest timestamp to the target.

- `formatted_timestamp(timestamp)`:
  - Formats the timestamp to the nearest minute out of 00, 12, 24, 36, 48.
  - Parameters:
    - `timestamp` (datetime): The timestamp to format.
  - Returns the formatted timestamp.

- `get_verfification_level(has_dimming, has_flare, flare_class, flare_threshold=25)`:
  - Determines the verification score based on the presence of dimmings and flares and the flare class.
  - Parameters:
    - `has_dimming` (bool): Indicates if a dimming is present.
    - `has_flare` (bool): Indicates if a flare is present.
    - `flare_class` (float): The flare class score.
    - `flare_threshold` (int): The threshold for the flare class score.
  - Returns the verification score.

