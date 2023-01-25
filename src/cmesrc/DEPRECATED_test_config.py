from src.cmesrc import config
from os.path import exists

def test_data_dir():
    assert exists(config.DATA_DIR)
    
def test_raw_data_dir():
    assert exists(config.RAW_DATA_DIR)
    
def test_interim_data_dir():
    assert exists(config.INTERIM_DATA_DIR)

def test_processed_data_dir():
    assert exists(config.PROCESSED_DATA_DIR)

def test_existence_raw_cme_catalogue():
    assert exists(config.RAW_LASCO_CME_CATALOGUE)

def test_existence_LASCO_dir():
    assert exists(config.LASCO_DATA_DIR)

def test_existence_MVTS_dir():
    assert exists(config.MVTS_DATA_DIR)

def test_existence_SWAN_dir():
    assert exists(config.SWAN_DATA_DIR)

def test_existence_partitions_dir():
    assert exists(config.PARTITIONS_DATA_DIR)

