from pathlib import Path

# Paths to directories

CONFIG_PARENT_DIR_PATH = Path(__file__).parent
ROOT = CONFIG_PARENT_DIR_PATH.joinpath(Path("../../"))

DATA_DIR = Path(ROOT.joinpath(Path("data/")))
RAW_DATA_DIR = Path(DATA_DIR.joinpath(Path("./raw/")))
INTERIM_DATA_DIR = Path(DATA_DIR.joinpath(Path("./interim/")))
PROCESSED_DATA_DIR = Path(DATA_DIR.joinpath(Path("./processed/")))

# Raw data files

RAW_LASCO_CME_CATALOGUE = Path(RAW_DATA_DIR.joinpath("./univ_all.txt"))
