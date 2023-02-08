import pandas as pd
from src.cmesrc.config import RAW_DIMMINGS_CATALOGUE, TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE
import matplotlib.pyplot as plt
import numpy as np
from astropy.time import Time
import astropy.units as u
import matplotlib.colors as colors

#|%%--%%| <l4vjwwU9iL|SKBzwIXFJw>

matching_dimmings_df = pd.read_pickle(TEMPORAL_MATCHING_DIMMINGS_DATABASE_PICKLE)

matching_dimmings_ids = matching_dimmings_df["matching_dimming_ids"].to_list()

matching_dimmings_set = set([item for sublist in matching_dimmings_ids for item in sublist])

matching_dimmings_ids = sorted(list(matching_dimmings_set))

dimmings = pd.read_csv(RAW_DIMMINGS_CATALOGUE)
all_dimming_ids = set(dimmings['dimming_id'].to_list())
dimmings.set_index('dimming_id', inplace=True)
dimmings.columns

non_matching_dimmings_ids = list(all_dimming_ids - matching_dimmings_set)

#|%%--%%| <SKBzwIXFJw|kVNRdnveph>

matching_dimmings_ids

#|%%--%%| <kVNRdnveph|Ibl0FX3inj>

dimmings["pixel_ratio"] = dimmings["max_num_pixels"] / dimmings["avg_num_pixels"]

start_time = dimmings["start_time"].to_list()
end_time = dimmings["end_time"].to_list()
half_value_time = dimmings["half_value_time"].to_list()
dimmings["start_time"] = Time(start_time)
dimmings["end_time"] = Time(end_time)
dimmings["half_value_time"] = Time(half_value_time)
dimmings["mid_time"] = dimmings["start_time"] + (dimmings["end_time"] - dimmings["start_time"]) / 2
dimmings["duration"] = [value.to(u.hour).value for value in (dimmings["end_time"] - dimmings["start_time"])]

#|%%--%%| <Ibl0FX3inj|fpcK9a3S3u>

matching_dimmings = dimmings.loc[matching_dimmings_ids]
not_matching_dimmings = dimmings.loc[non_matching_dimmings_ids]

#|%%--%%| <fpcK9a3S3u|V3HPLxuorr>

fig, ax = plt.subplots()

BINS = 50
RANGE = [0,100]

COLUMN = "avg_median_intensity_abs"

ax.hist(not_matching_dimmings[COLUMN], bins=BINS, range=RANGE, density=True, histtype="step", label="not_matching")
ax.hist(matching_dimmings[COLUMN], bins=BINS, range=RANGE, density=True, histtype="step", label="Matching")

fig.legend()

#matching_dimmings.hist(["duration"], bins=100, label="MATCHING", ax = ax)

#|%%--%%| <V3HPLxuorr|ws2p8sKfTu>


fig, ax = plt.subplots()

BINS = 50
RANGE = [10,10000]

COLUMN = "avg_num_pixels"

ax.hist(not_matching_dimmings[COLUMN], bins=BINS, range=RANGE, density=True, histtype="step", label="not_matching")
ax.hist(matching_dimmings[COLUMN], bins=BINS, range=RANGE, density=True, histtype="step", label="Matching")

fig.legend()

#|%%--%%| <ws2p8sKfTu|BGMUBaQVYw>

dimmings.hist(["max_num_pixels"], bins=200)

#|%%--%%| <BGMUBaQVYw|Aa4mie6BNC>

fig, axes = plt.subplots(dpi=200, nrows=3, figsize=(5,10), sharex=True)

a = dimmings[["longitude", "latitude"]].dropna()
b = matching_dimmings[["longitude", "latitude"]].dropna()
c = not_matching_dimmings[["longitude", "latitude"]].dropna()

axes[0].hist2d(a["longitude"], a["latitude"], bins=50, range=[[-90, 90], [-90, 90]], density=True)
axes[1].hist2d(b["longitude"], b["latitude"], bins=50, range=[[-90, 90], [-90, 90]], density=True)
axes[2].hist2d(c["longitude"], c["latitude"], bins=50, range=[[-90, 90], [-90, 90]], density=True)
