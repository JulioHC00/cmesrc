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
PARTITIONS_DATA_DIR = os.path.join(MVTS_DATA_DIR, "partitions/")

# Raw data files

RAW_LASCO_CME_CATALOGUE = os.path.join(LASCO_DATA_DIR, "univ_all.txt")

# Interim data files

HARPS_LIFETIME_DATABSE = os.path.join(INTERIM_DATA_DIR, "harps_lifetime_database.csv")
LASCO_CME_DATABASE = os.path.join(INTERIM_DATA_DIR, "lasco_cme_database.csv")
TEMPORAL_MATCHING_HARPS_DATABASE = os.path.join(INTERIM_DATA_DIR, "temporal_matching_harps_database.csv")
SPATIOTEMPORAL_MATCHING_HARPS_DATABASE = os.path.join(INTERIM_DATA_DIR, "spatiotemporal_matching_harps_database.csv")
