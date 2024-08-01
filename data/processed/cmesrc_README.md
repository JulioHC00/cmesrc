# CMESRC Database Schema Documentation

This document provides a detailed description of each table and column in the CMESRC database.

## Tables

### HARPS

- **Description**: Contains information about each HARP region.
- **Columns**:
  - `harpnum` (INTEGER PRIMARY KEY): Unique identifier for each HARP region.
  - `start` (TEXT NOT NULL): Start timestamp of the HARP region.
  - `end` (TEXT NOT NULL): End timestamp of the HARP region.
  - `area` (FLOAT): Area of the HARP region as a fraction of half the solar disk.
  - `n_noaas` (INTEGER): Number of NOAA active regions in the HARP region.

### CMES

- **Description**: Contains information about each Coronal Mass Ejection (CME).
- **Columns**:
  - `cme_id` (INTEGER NOT NULL PRIMARY KEY): Unique identifier for each CME.
  - `cme_date` (TEXT NOT NULL): Date and time of the CME.
  - `cme_pa` (REAL): Position angle of the CME.
  - `cme_width` (REAL NOT NULL): Width of the CME.
  - `cme_halo` (INTEGER): Indicator for Halo CMEs (1 for Halo, else NULL).
  - `cme_seen_in` (INTEGER NOT NULL): Where the CME was observed (e.g., C2, C3).
  - `cme_three_points` (INTEGER NOT NULL): Number of observed points for the CME.
  - `cme_quality` (INTEGER NOT NULL): Quality rating for the CME observation.
  - `image_timestamp` (TEXT): Timestamp of the associated image.

### FLARES

- **Description**: Contains information about each solar flare.
- **Columns**:
  - `flare_id` (INTEGER NOT NULL PRIMARY KEY): Unique identifier for each flare.
  - `harpnum` (INTEGER REFERENCES HARPS (harpnum)): Associated HARP region.
  - `flare_date` (TEXT NOT NULL): Date and time of the flare.
  - `flare_lon` (REAL): Longitude of the flare.
  - `flare_lat` (REAL): Latitude of the flare.
  - `flare_class_score` (REAL NOT NULL): Score indicating the class of the flare.
  - `flare_class` (TEXT NOT NULL): Class of the flare (e.g., M1, X2).
  - `flare_ar` (INTEGER): Active Region number of the flare.
  - `flare_ar_source` (TEXT): Source of the Active Region number.
  - `flare_verification` (TEXT): Verification status of the flare information.
  - `image_timestamp` (TEXT): Timestamp of the associated image.

### RAW_HARPS_BBOX

- **Description**: Contains raw bounding box data for HARP regions.
- **Columns**:
  - `harpnum` (INTEGER REFERENCES HARPS (harpnum)): Reference to the harpnum column in the harps table.
  - `timestamp` (TEXT): Reference to the timestamp column in the images table.
  - `LONDTMIN` (REAL): Minimum longitude of BBOX.
  - `LONDTMAX` (REAL): Maximum longitude of BBOX.
  - `LATDTMIN` (REAL): Minimum latitude of BBOX.
  - `LATDTMAX` (REAL): Maximum latitude of BBOX.
  - `IRBB` (INTEGER): Specifies if the BBOX is calculated using differential solar rotation.
  - `IS_TMFI` (INTEGER): Specifies if the magnetic field data is trusted.

### FINAL_CME_HARP_ASSOCIATIONS

- **Description**: Contains final associations between CMEs and HARP regions.
- **Columns**:
  - `cme_id` (INTEGER UNIQUE NOT NULL REFERENCES CMES(cme_id)): Unique identifier for each CME.
  - `harpnum` (INTEGER NOT NULL REFERENCES HARPS(harpnum)): Associated HARP region.
  - `association_method` (TEXT NOT NULL): Method used to determine the association.
  - `verification_score` (REAL): Confidence score of the association.
  - `independent_verified` (INTEGER NOT NULL DEFAULT 0): Meant to be used if CMEs are verified beyond automatic association. Not in use

### OVERLAPS

- **Description**: Contains information about overlaps between HARP regions.
- **Columns**:
  - `harpnum_a` (INTEGER REFERENCES HARPS(harpnum)): First HARP region in the overlap.
  - `harpnum_b` (INTEGER REFERENCES HARPS(harpnum)): Second HARP region in the overlap.
  - `mean_overlap` (REAL): Mean overlap percentage between the two HARP regions.
  - `mean_pixel_overlap` (REAL): Mean pixel overlap percentage between the two HARP regions.
  - `ocurrence_percentage` (REAL): Occurrence percentage of the overlap.
  - `pixel_ocurrence_percentage` (REAL): Pixel occurrence percentage of the overlap.
  - `harpnum_a_area` (REAL): Area of the first HARP region.
  - `harpnum_a_pixel_area` (REAL): Pixel area of the first HARP region.
  - `harpnum_b_area` (REAL): Area of the second HARP region.
  - `coexistence` (REAL): Coexistence value of the overlap.

### OVERLAP_RECORDS

- **Description**: Contains records of overlap decisions between HARP regions.
- **Columns**:
  - `harpnum_a` (INTEGER REFERENCES HARPS(harpnum)): First HARP region in the overlap.
  - `harpnum_b` (INTEGER REFERENCES HARPS(harpnum)): Second HARP region in the overlap.
  - `decision` (STRING): Decision made regarding the overlap.
  - `mean_overlap` (REAL): Mean overlap percentage between the two HARP regions.
  - `std_overlap` (REAL): Standard deviation of the overlap.
  - `ocurrence_percentage` (REAL): Occurrence percentage of the overlap.
  - `harpnum_a_area` (REAL): Area of the first HARP region.
  - `harpnum_b_area` (REAL): Area of the second HARP region.
  - `b_over_a_area_ratio` (REAL): Area ratio of the second HARP region over the first.

### PROCESSED_HARPS_BBOX

- **Description**: Contains processed bounding box data for HARP regions.
- **Columns**:
  - `harpnum` (INTEGER REFERENCES HARPS (harpnum)): Reference to the harpnum column in the harps table.
  - `timestamp` (TEXT): Reference to the timestamp column in the images table.
  - `LONDTMIN` (REAL): Minimum longitude of BBOX.
  - `LONDTMAX` (REAL): Maximum longitude of BBOX.
  - `LATDTMIN` (REAL): Minimum latitude of BBOX.
  - `LATDTMAX` (REAL): Maximum latitude of BBOX.
  - `IRBB` (INTEGER): Specifies if the BBOX is calculated using differential solar rotation.
  - `IS_TMFI` (INTEGER): Specifies if the magnetic field data is trusted.

### CMES_HARPS_EVENTS

- **Description**: Contains events associated with CMEs and HARP regions.
- **Columns**:
  - `harpnum` (INTEGER): HARP region number.
  - `cme_id` (INTEGER): CME identifier.
  - `flare_id` (INTEGER REFERENCES FLARES(flare_id)): Flare identifier.
  - `flare_hours_diff` (INTEGER NOT NULL): Hours difference between the CME and the flare.
  - `dimming_id` (INTEGER REFERENCES DIMMINGS(dimming_id)): Dimming identifier.
  - `dimming_hours_diff` (INTEGER NOT NULL): Hours difference between the CME and the dimming.

### CMES_HARPS_SPATIALLY_CONSIST

- **Description**: Contains spatially consistent associations between CMEs and HARP regions.
- **Columns**:
  - `harpnum` (INTEGER NOT NULL REFERENCES HARPS(harpnum)): HARP region number.
  - `cme_id` (INTEGER NOT NULL REFERENCES CMES(cme_id)): CME identifier.

### DIMMINGS

- **Description**: Contains information about each dimming event.
- **Columns**:
  - `dimming_id` (INTEGER NOT NULL): Unique identifier for each dimming event.
  - `harpnum` (INTEGER REFERENCES HARPS(harpnum)): Associated HARP region.
  - `harps_dimming_dist` (REAL NOT NULL): Distance between the HARP region and the dimming event.
  - `dimming_start_date` (TEXT NOT NULL): Start date of the dimming event.
  - `dimming_peak_date` (TEXT NOT NULL): Peak date of the dimming event.
  - `dimming_lon` (REAL NOT NULL): Longitude of the dimming event.
  - `dimming_lat` (REAL NOT NULL): Latitude of the dimming event.
  - `image_timestamp` (TEXT): Timestamp of the associated image.

### NOAA_HARPNUM_MAPPING

- **Description**: Contains mapping between NOAA active regions and HARP regions.
- **Columns**:
  - `noaa` (INTEGER): NOAA active region number.
  - `harpnum` (INTEGER REFERENCES HARPS (harpnum)): HARP region number.

### NOAAS

- **Description**: Contains a list of NOAA active regions.
- **Columns**:
  - `noaa` (INTEGER PRIMARY KEY): NOAA active region number.
