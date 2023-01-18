from pathlib import Path

# Paths to directories
data_dir = Path("../../data/")
raw_data_dir = Path(data_dir.joinpath(Path("./raw/")))
interim_data_dir = Path(data_dir.joinpath(Path("./interim/")))
processed_data_dir = Path(data_dir.joinpath(Path("./processed/")))
