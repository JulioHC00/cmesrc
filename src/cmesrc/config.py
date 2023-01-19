from pathlib import Path

# Paths to directories

CONFIG_PARENT_DIR_PATH = Path(__file__).parent
ROOT = CONFIG_PARENT_DIR_PATH.joinpath(Path("../../"))

DATA_DIR = Path(ROOT.joinpath(Path("data/")))
RAW_DATA_DIR = Path(DATA_DIR.joinpath(Path("./raw/")))
INTERIM_DATA_DIR = Path(DATA_DIR.joinpath(Path("./interim/")))
PROCESSED_DATA_DIR = Path(DATA_DIR.joinpath(Path("./processed/")))
LASCO_DATA_DIR = Path(RAW_DATA_DIR.joinpath(Path("./lasco/")))
MVTS_DATA_DIR = Path(RAW_DATA_DIR.joinpath(Path("./mvts/")))
SWAN_DATA_DIR = Path(MVTS_DATA_DIR.joinpath(Path("./SWAN/")))
PARTITIONS_DATA_DIR = Path(MVTS_DATA_DIR.joinpath(Path("./partitions/")))

# Raw data files

RAW_LASCO_CME_CATALOGUE = Path(LASCO_DATA_DIR.joinpath("./univ_all.txt"))

# Interim data files

HARPS_LIFETIME_DATABSE = Path(INTERIM_DATA_DIR.joinpath("./harps_lifetime_database.csv"))
LASCO_CME_DATABASE = Path(INTERIM_DATA_DIR.joinpath("./lasco_cme_database.csv"))
