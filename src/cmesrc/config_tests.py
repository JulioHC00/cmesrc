from src.cmesrc import config

def test_data_dir():
    assert config.data_dir.exists()
    
def test_raw_data_dir():
    assert config.raw_data_dir.exists()
    
def test_interim_data_dir():
    assert config.interim_data_dir.exists()

def test_processed_data_dir():
    assert config.processed_data_dir.exists()
