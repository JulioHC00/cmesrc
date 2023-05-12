import pandas as pd
from src.cmesrc.config import MAIN_DATABASE_PICKLE

main_database = pd.read_pickle(MAIN_DATABASE_PICKLE)

dimming_ids = main_database["DIMMING_MATCH"]
flare_ids = main_database["FLARE_MATCH"]

if len(dimming_ids) != len(set(dimming_ids)):
    dimmings_check = "X"
else:
    dimmings_check = "O"

if len(flare_ids) != len(set(flare_ids)):
    flares_check = "X"
else:
    flares_check = "O"

print(f"CHECK | RES\n\n----\t+\t-----\nFLARE\t{flares_check}\nDIMMING\t{dimmings_check}")