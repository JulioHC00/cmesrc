from pathlib import Path
from os.path import dirname, abspath, join

# Paths to directories

ROOT = Path(dirname(abspath("requirements.txt")))

DATA_DIR = ROOT.joinpath("data/")
RAW_DATA_DIR = DATA_DIR.joinpath("./raw/")
INTERIM_DATA_DIR = DATA_DIR.joinpath("./interim/")
PROCESSED_DATA_DIR = DATA_DIR.joinpath("./processed/")
LASCO_DATA_DIR = RAW_DATA_DIR.joinpath("./lasco/")
MVTS_DATA_DIR = RAW_DATA_DIR.joinpath("./mvts/")
SWAN_DATA_DIR = MVTS_DATA_DIR.joinpath("./SWAN/")
PARTITIONS_DATA_DIR = MVTS_DATA_DIR.joinpath("./partitions/")

# Raw data files

RAW_LASCO_CME_CATALOGUE = Path(LASCO_DATA_DIR.joinpath("./univ_all.txt"))

# Interim data files

HARPS_LIFETIME_DATABSE = Path(INTERIM_DATA_DIR.joinpath("./harps_lifetime_database.csv"))
LASCO_CME_DATABASE = Path(INTERIM_DATA_DIR.joinpath("./lasco_cme_database.csv"))
TEMPORAL_MATCHING_HARPS_DATABASE = Path(INTERIM_DATA_DIR.joinpath("./temporal_matching_harps_database.csv"))
