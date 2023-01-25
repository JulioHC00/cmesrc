from src.cmesrc import config

def test_data_dir():
    assert config.DATA_DIR.exists()
    
def test_raw_data_dir():
    assert config.RAW_DATA_DIR.exists()
    
def test_interim_data_dir():
    assert config.INTERIM_DATA_DIR.exists()

def test_processed_data_dir():
    assert config.PROCESSED_DATA_DIR.exists()

def test_existence_raw_cme_catalogue():
    assert config.RAW_LASCO_CME_CATALOGUE.exists()

def test_existence_LASCO_dir():
    assert config.LASCO_DATA_DIR.exists()

def test_existence_MVTS_dir():
    assert config.MVTS_DATA_DIR.exists()

def test_existence_SWAN_dir():
    assert config.SWAN_DATA_DIR.exists()

def test_existence_partitions_dir():
    assert config.PARTITIONS_DATA_DIR.exists()

