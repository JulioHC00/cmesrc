from pathlib import Path

# Paths to directories

CONFIG_PARENT_DIR_PATH = Path(__file__).parent
ROOT = CONFIG_PARENT_DIR_PATH.joinpath(Path("../../"))

DATA_DIR = ROOT.joinpath(Path("data/"))
RAW_DATA_DIR = DATA_DIR.joinpath("./raw/")
INTERIM_DATA_DIR = DATA_DIR.joinpath(Path("./interim/"))
PROCESSED_DATA_DIR = DATA_DIR.joinpath(Path("./processed/"))
LASCO_DATA_DIR = RAW_DATA_DIR.joinpath(Path("./lasco/"))
MVTS_DATA_DIR = RAW_DATA_DIR.joinpath(Path("./mvts/"))
SWAN_DATA_DIR = MVTS_DATA_DIR.joinpath(Path("./SWAN/"))
PARTITIONS_DATA_DIR = MVTS_DATA_DIR.joinpath(Path("./partitions/"))

# Raw data files

RAW_LASCO_CME_CATALOGUE = Path(LASCO_DATA_DIR.joinpath("./univ_all.txt"))

# Interim data files

HARPS_LIFETIME_DATABSE = Path(INTERIM_DATA_DIR.joinpath("./harps_lifetime_database.csv"))
LASCO_CME_DATABASE = Path(INTERIM_DATA_DIR.joinpath("./lasco_cme_database.csv"))
TEMPORAL_MATCHING_HARPS_DATABASE = Path(INTERIM_DATA_DIR.joinpath("./temporal_matching_harps_database.csv"))
