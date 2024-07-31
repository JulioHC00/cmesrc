# Identifying Coronal Mass Ejection Active Region Sources: An automated approach

## Overview

This GitHub repository contains the source code for **NEED PAPER DOI**. We provide detailed README files for relevant files. While the main points required to understand how our CME active region source identification algorithm works is detailed in this README file, the individual file READMEs provide more detailed information on the process each file performs. We also provide a Makefile that allows reproducing our results by running the files in the correct order. The final result is a database with the relevant results. A README is provided for the database as well, detailing the contents of each table.

## Downloading the results

If you're just interested in the final list of CMEs matched to their source regions, you can either download the whole database from **MISSING LINK** or a .csv version that contains just the list of CMEs and their associated SHARPs from **MISSING_LINK**

## Reproducing our results

### Prerequisites

Start by cloning the repository to your local machine. You can do this by running the following command in your terminal:

```bash
git clone https://github.com/JulioHC00/cmesrc.git
```

Once you have the repository cloned, cd into the folder using `cd cmesrc` and run

```bash
make create_environment
```

If you get an error message indicating that you're missing `pip`, please install it first before running this command. This command will

1. Ensure `pip` is installed
2. Ensure `env` is installed. If it isn't, it will install it using `pip`
3. Create a virtual environment called `env`

You should now activate the virtual environment using

```bash
source env/bin/activate
```

And finally install the dependencies listed in `requirements.txt` by doing

```bash
make install_requirements
```

You can then finally run the pipeline using

```bash
make generate_catalogue
```

Note depending on your system, this will take from ~5 minutes to ~15 minutes.

## Key files

Here we detail key files in the pipeline in the order they're run by the Makefile and provide a short description. Each has an accompanying README file with more detailed information. All of these files are located at `src/scripts`

1. `pre-processing/fill_swan_missing_positions.py`: While SHARPs bounding boxes are recorded every 12 minutes, there are gaps in the data. We fill these missing positions using the closest bounding box and rotating it taking into account solar differential rotation through `SunPy`.
2. `catalogue/pre_data_loading.py`: We load the bounding boxes into a database and perform some extra pre-processing on them. This consists in removing bounding boxes that cover an excessively large area of the Sun and are most likely erroneous. We also take care of regions which overlap significantly in area and time by keeping only the largest of them. For full details check the original file and its associated README.
3. `pre-processing/extract_harps_lifetimes.py`: We extract the first and last timestamp for each SHARP region and save it to the database.
4. `pre-processing/parse_lasco_cme_catalogue.py`: We parse the raw `.txt` version of the LASCO CME catalogue and save it in a `.csv`
5. `spatiotemporal_matching/temporal_matching.py`: For each CME, we find the SHARP regions which were present in the solar disk.
6. `spatiotemporal_matching/spatial_matching.py`: For each CME and region from the previous step, we check whether the region falls within a wedge centred at the CME position angle with a width equal to the CME width plus some padding. If it does, we consider the region to be spatiotemporally matching with the CME.
7. `dimmings/match_dimmings_to_harps.py`: We match dimmings from Solar Demon by finding the closest region to it. We only consider regions that are within a great circle distance of 10 degrees. If no such region exists, the dimming is unmatched and not included beyond this point.
8. `flares/match_flares_to_harps.py`: Although the name of this file may suggest we match the flares to SHARP regions ourselves, this is already done in the raw data we use. So we just extract the flares and their associated SHARP region and store it.
9. `catalogue/generate_catalogue.py`: The final script takes all the previous results to produce the final matches. For each CME, it considers the spatiotemporally matching regions. For each region, it finds any dimming or flare that happened withing the two hours before the CME was detected in LASCO and ranks the regions if there's more than one. The best scoring one is chosen. All the results are stored in the database.

## How to understand why a CME was matched to a particular region

In order to understand the association of a CME with a region (or why it wasn't associated) we recommend using a tool like ![sqlitebrowser](https://sqlitebrowser.org/) and follow these steps

1. Find your CME in the `CMES` table. Is it within the dates covered by our study? If not it won't be matched. Does it have a `cme_quality` different to 0? Then we won't have considered it for matching. Otherwise, take the `cme_id` value.
2. In the table `CMES_HARPS_SPATIALLY_CONSIST`, use the `cme_id` to filter the rows. You will then see all the regions that were found to be spatiotemporally consistent with this CME. If there are no rows, this means there was no spatiotemporally matching region found and we couldn't match this CME.
3. Next, use the `cme_id` to filter the rows of the table `CMES_HARPS_EVENTS`. This will tell you for each CME-SHARPs pair the events (dimming, flare) that are potentially associated with the CME for that particular region. You can use the flare and dimmings ids to filter the `DIMMINGS` and `FLARES` tables to get more information on them. These tables also allow you to see all the flares and dimmings associated with a particular region and understand why a particular event may not have been considered for a CME.
4. Using the rules described in the paper, all the pairs from the previous steps will be ranked. The highest ranking on will be recorded as the source and stored in `FINAL_CME_HARP_ASSOCIATIONS`. Note that if no pair had any signature, it won't be recorded.

# Acknowledgements
