from src.cmesrc.utils import cache_swan_data
from src.cmesrc.config import DT_SWAN_DATA_DIR
import pandas as pd
from astropy.time import Time
from os.path import join
from tqdm import tqdm
import drms

client = drms.Client(email="ucapern@ucl.ac.uk", verbose=True)

SWAN = cache_swan_data()

for harpnum, data in tqdm(SWAN.items()):

#    df = pd.read_csv(f"/home/julio/cmesrc/data/interim/SWAN/{harpnum}.csv", sep="\t")

    data.drop(columns=["Timestamp"], inplace=True)

    hmi_request = f"hmi.sharp_720s[{harpnum}]"

    keywords = client.query(hmi_request, key="LONDTMIN, LONDTMAX, LATDTMIN, LATDTMAX, T_REC")

    keywords["Timestamp"] = Time(keywords["T_REC"].apply(lambda x: x[:-4].replace(".","-").replace("_"," ")).to_list())
    keywords.drop(columns=["T_REC"], inplace=True)

    data = pd.merge(data, keywords, on="Timestamp", how="outer")

    filename = f"{harpnum}.csv"

    data.to_csv(join(DT_SWAN_DATA_DIR, filename), sep="\t", index=False)