from pathlib import Path
import os.path

# Paths to directories

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_DIR = os.path.join(ROOT, "data/")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw/")
INTERIM_DATA_DIR = os.path.join(DATA_DIR, "interim/")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed/")
LASCO_DATA_DIR = os.path.join(RAW_DATA_DIR, "lasco/")
MVTS_DATA_DIR = os.path.join(RAW_DATA_DIR, "mvts/")
SWAN_DATA_DIR = os.path.join(MVTS_DATA_DIR, "SWAN/")
DT_SWAN_DATA_DIR = os.path.join(MVTS_DATA_DIR, "DT_SWAN/")
PARTITIONS_DATA_DIR = os.path.join(MVTS_DATA_DIR, "partitions/")
DIMMINGS_DATA_DIR = os.path.join(RAW_DATA_DIR, "dimmings/")
FLARES_DATA_DIR = os.path.join(RAW_DATA_DIR, "flares/")
REPORTS_DIR = os.path.join(ROOT, "reports/")
FIGURES_DIR = os.path.join(REPORTS_DIR, "figures/")
OVERVIEW_FIGURES_DIR = os.path.join(FIGURES_DIR, "overviews/")
SDOML_FOLDER = os.path.join(RAW_DATA_DIR, "sdoml/")

# Raw data files

RAW_LASCO_CME_CATALOGUE = os.path.join(LASCO_DATA_DIR, "univ_all.txt")
RAW_DIMMINGS_CATALOGUE = os.path.join(DIMMINGS_DATA_DIR, "dimmings.csv")
RAW_FLARE_CATALOGUE = os.path.join(FLARES_DATA_DIR, "goes_sxr_flares.csv")

SDOML_TIMESTAMP_INFO = os.path.join(SDOML_FOLDER, "timestamps_info_dict.pkl")

# Interim data files

HARPS_LIFETIME_DATABSE = os.path.join(INTERIM_DATA_DIR, "harps_lifetime_database.csv")
LASCO_CME_DATABASE = os.path.join(INTERIM_DATA_DIR, "lasco_cme_database.csv")
TEMPORAL_MATCHING_HARPS_DATABASE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_harps_database.csv")
TEMPORAL_MATCHING_HARPS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_harps_database.pkl")
SPATIOTEMPORAL_MATCHING_HARPS_DATABASE = os.path.join(INTERIM_DATA_DIR, "spatiotemporal_matching_harps_database.csv")
SPATIOTEMPORAL_MATCHING_HARPS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "spatiotemporal_matching_harps_database.pkl")
ALL_MATCHING_HARPS_DATABASE = os.path.join(INTERIM_DATA_DIR, "all_matching_harps_database.csv")
ALL_MATCHING_HARPS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "all_matching_harps_database.pkl")
TEMPORAL_MATCHING_DIMMINGS_DATABASE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_dimmings_database.csv")
TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_dimmings_database.pkl")
HARPS_MATCHING_DIMMINGS_DATABASE = os.path.join(INTERIM_DATA_DIR, "harps_matching_dimmings_database.csv")
HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "harps_matching_dimmings_database.pkl")
PLOTTING_DATABASE = os.path.join(INTERIM_DATA_DIR, "plotting_database.csv")
PLOTTING_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "plotting_database.pkl")

SCORED_HARPS_MATCHING_DIMMINGS_DATABASE = os.path.join(INTERIM_DATA_DIR, "scored_harps_matching_dimmings_database.csv")
SCORED_HARPS_MATCHING_DIMMINGS_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "scored_harps_matching_dimmings_database.pkl")

TEMPORAL_MATCHING_FLARES_DATABASE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_flares_database.csv")
TEMPORAL_MATCHING_FLARES_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_flares_database.pkl")
HARPS_MATCHING_FLARES_DATABASE = os.path.join(INTERIM_DATA_DIR, "harps_matching_flares_database.csv")
HARPS_MATCHING_FLARES_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "harps_matching_flares_database.pkl")

SCORED_HARPS_MATCHING_FLARES_DATABASE = os.path.join(INTERIM_DATA_DIR, "scored_harps_matching_flares_database.csv")
SCORED_HARPS_MATCHING_FLARES_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "scored_harps_matching_flares_database.pkl")

MAIN_DATABASE_PICKLE = os.path.join(INTERIM_DATA_DIR, "main_database.pkl")
MAIN_DATABASE = os.path.join(INTERIM_DATA_DIR, "main_database.csv")

DIMMINGS_MATCHED_TO_HARPS = os.path.join(INTERIM_DATA_DIR, "dimmings_matched_to_harps.csv")
DIMMINGS_MATCHED_TO_HARPS_PICKLE = os.path.join(INTERIM_DATA_DIR, "dimmings_matched_to_harps.pkl")

FLARES_MATCHED_TO_HARPS = os.path.join(INTERIM_DATA_DIR, "flares_matched_to_harps.csv")
FLARES_MATCHED_TO_HARPS_PICKLE = os.path.join(INTERIM_DATA_DIR, "flares_matched_to_harps.pkl")

UPDATED_SWAN = os.path.join(INTERIM_DATA_DIR, "SWAN/")

CMESRC_DB = os.path.join(PROCESSED_DATA_DIR, 'cmesrc.db')
CMESRCV2_DB = os.path.join(PROCESSED_DATA_DIR, 'cmesrcV2.db')
CMESRCV3_DB = os.path.join(PROCESSED_DATA_DIR, 'cmesrcV3.db')
GENERAL_DATASET = os.path.join(PROCESSED_DATA_DIR, 'general_dataset.db')
PIXEL_BBOXES = os.path.join(PROCESSED_DATA_DIR, "pixel_bbox.db")
SDOML_DATASET = os.path.join(PROCESSED_DATA_DIR, "sdoml_dataset.db")

MAJUMDAR_CATALOGUE = os.path.join(INTERIM_DATA_DIR, "majumdar/CSR_catalogue.txt")
SANJIV_CATALOGUE = os.path.join(RAW_DATA_DIR, "sanjiv/sanjiv_catalogue.csv")
HARPNUM_TO_NOAA = os.path.join(RAW_DATA_DIR, "harpnum_to_noaa/all_harps_with_noaa_ars.txt")
RESEARCH_LOG = "/home/julio/research_log/"
ZARR_BASE_PATH = "/home/julio/cutouts/cutouts/"