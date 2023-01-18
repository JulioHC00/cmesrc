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
