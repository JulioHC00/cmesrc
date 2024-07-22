# Makefile `make all` Steps

This document outlines the steps executed when running `make all` in the project's `Makefile`.

## Steps

1. **Fill Missing SWAN Positions**:
   - Description: This script fills in missing positions in the SWAN dataset by interpolating or using other methods.
   - Script: `./src/scripts/pre-processing/fill_swan_missing_positions.py`
   - Input: CSV files from `./data/raw/mvts/DT_SWAN/`
   - Output: Updated CSV files in `./data/interim/SWAN/`

2. **Create HARPS Lifetime Database**:
   - Description: This script extracts the lifetime data of HARPS from the updated SWAN files and creates a database.
   - Script: `./src/scripts/pre-processing/extract_harps_lifetimes.py`
   - Input: Updated SWAN CSV files from `./data/interim/SWAN/`
   - Output: `./data/interim/harps_lifetime_database.csv`

3. **Parse LASCO CME Database**:
   - Description: This script parses the LASCO CME catalogue to extract relevant data into a CSV format.
   - Script: `./src/scripts/pre-processing/parse_lasco_cme_catalogue.py`
   - Input: `./data/raw/lasco/univ_all.txt`
   - Output: `./data/interim/lasco_cme_database.csv`

4. **Temporally Matching HARPS**:
   - Description: This script matches HARPS data temporally with the LASCO CME database.
   - Script: `./src/scripts/spatiotemporal_matching/temporal_matching.py`
   - Input: `./data/interim/lasco_cme_database.csv`, `./data/interim/harps_lifetime_database.csv`, `./src/cmesrc/classes.py`, `./src/harps/harps.py`, `./src/cmes/cmes.py`
   - Output: `./data/interim/temporal_matching_harps_database.csv`

5. **Spatiotemporally Matching HARPS**:
   - Description: This script further matches HARPS data spatiotemporally based on the temporal matching results.
   - Script: `./src/scripts/spatiotemporal_matching/spatial_matching.py`
   - Input: `./data/interim/temporal_matching_harps_database.csv`
   - Output: `./data/interim/spatiotemporal_matching_harps_database.csv`

6. **Match Dimmings**:
   - Description: This script matches dimming events to the HARPS database.
   - Script: `./src/scripts/dimmings/match_dimmings_to_harps.py`
   - Input: `./data/interim/spatiotemporal_matching_harps_database.csv`, `./src/dimmings/dimmings.py`, `./data/raw/dimmings/dimmings.csv`
   - Output: `./data/interim/dimmings_matched_to_harps.csv`

7. **Match Flares**:
   - Description: This script matches flare events to the HARPS database.
   - Script: `./src/scripts/flares/match_flares_to_harps.py`
   - Input: `./data/interim/spatiotemporal_matching_harps_database.csv`, `./src/flares/flares.py`, `./data/raw/flares/goes_sxr_flares.csv`
   - Output: `./data/interim/flares_matched_to_harps.csv`

8. **[DEPRECATED] Collate Results**:
   - Description: This step is deprecated. Use `src/scripts/catalogue/generate_catalogue.py` instead.
   - Script: `./src/scripts/collect_results/collate_results.py`
   - Input: `./data/interim/spatiotemporal_matching_harps_database.csv`, `./data/interim/dimmings_matched_to_harps.csv`, `./data/interim/flares_matched_to_harps.csv`
   - Output: `./data/interim/main_database.csv`
