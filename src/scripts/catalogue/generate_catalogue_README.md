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
   - Associates dimmings, flares, and CMEs based on temporal and spatial criteria.
   - Determines the verification level for each association.
   - Saves the final associations into the database.

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
  - Determines the verification level based on the presence of dimmings and flares and the flare class.
  - Parameters:
    - `has_dimming` (bool): Indicates if a dimming is present.
    - `has_flare` (bool): Indicates if a flare is present.
    - `flare_class` (float): The flare class score.
    - `flare_threshold` (int): The threshold for the flare class score.
  - Returns the verification level.

This script is crucial for creating a comprehensive catalogue of solar events, ensuring that the data is accurately loaded and associated for further analysis.
