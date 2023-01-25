from pathlib import Path
from os.path import dirname, abspath, join

# Paths to directories

ROOT = Path(dirname(abspath("../requirements.txt")))

DATA_DIR = join(ROOT, "data/")
RAW_DATA_DIR = join(DATA_DIR, "raw/")
INTERIM_DATA_DIR = join(DATA_DIR, "interim/")
PROCESSED_DATA_DIR = join(DATA_DIR, "processed/")
LASCO_DATA_DIR = join(RAW_DATA_DIR, "lasco/")
MVTS_DATA_DIR = join(RAW_DATA_DIR, "mvts/")
SWAN_DATA_DIR = join(MVTS_DATA_DIR, "SWAN/")
PARTITIONS_DATA_DIR = join(MVTS_DATA_DIR, "partitions/")

# Raw data files

RAW_LASCO_CME_CATALOGUE = join(LASCO_DATA_DIR, "univ_all.txt")

# Interim data files

HARPS_LIFETIME_DATABSE = join(INTERIM_DATA_DIR, "harps_lifetime_database.csv")
LASCO_CME_DATABASE = join(INTERIM_DATA_DIR, "lasco_cme_database.csv")
TEMPORAL_MATCHING_HARPS_DATABASE = join(INTERIM_DATA_DIR, "temporal_matching_harps_database.csv")
