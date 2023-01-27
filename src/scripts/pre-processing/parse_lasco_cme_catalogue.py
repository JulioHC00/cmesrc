"""
Parses the .txt file of the LASCO CME catalogue into a .csv format
"""

from src.cmesrc.config import RAW_LASCO_CME_CATALOGUE, INTERIM_DATA_DIR, LASCO_CME_DATABASE
import pandas as pd
import re
from pathlib import Path
import os.path

def parse_lasco_cme_catalogue():
    with open(RAW_LASCO_CME_CATALOGUE, "r") as file:
        lines = file.readlines()

    # Remove header
    lines = lines[4:]

    # We want to separate comments into a different list
    comments = [line[101:] for line in lines]
    data = [line[:101] for line in lines]

    # Pandas dataframe to hold the data
    finalDataFrame = pd.DataFrame(columns=["id",
                                           "date",
                                           "pa",
                                           "width",
                                           "linear_speed",
                                           "2nd_order_initial_speed",
                                           "2nd_order_final_speed",
                                           "2nd_order_20R_speed",
                                           "acceleration",
                                           "mass",
                                           "kinetic_energy",
                                           "MPA",
                                           "halo",
                                           "seen_in",
                                           "quality",
                                           "three_points"]
                                  )

    # Parse the data first
    rows = [] # List of all rows

    EMPTY_LINE = ""
    for line, comment in zip(data, comments):
        if line == EMPTY_LINE:
            continue

        raw_columns = line.split()

        # Some data may be missing (represented by ---- in the file).
        no_missing_values_columns = [column.replace("-","") for column in raw_columns]

        # Non_reliable measurements of acceleration or mass and kinetic energy
        # measurements with large uncertainties are marked with a *. We remove these as well.
        non_reliable_removed_columns = ['' if "*" in column else column for column in no_missing_values_columns]

        # Now, Halo CMES have the word "Halo" in the "Width column". For those, we change the width to "" and add a halo flag
        processed_columns = []
        halo = 0
        for column in non_reliable_removed_columns:
            if "Halo" in column:
                processed_columns.append("")
                halo = 1
            else:
                processed_columns.append(column)

        # Now, we define some flags based on the comments:
        # seen_in: 0 for C2 and C3, 1 for C2 only and 2 for C3 only
        # quality: 0 for no comments, 1 for Poor Event and 2 for Very Poor Event
        # only_n_points: 0 for no warning, n for n points

        onlyC2RePattern = "Only C2"
        onlyC3RePattern = "Only C3"

        if re.search(onlyC2RePattern, comment):
            seen_in = 1
        elif re.search(onlyC3RePattern, comment):
            seen_in = 2
        else:
            seen_in = 0

        veryPoorRePattern = "Very Poor"
        poorRePattern = "Poor"

        # Order of if statements important. FIRST check "Very Poort", if no match 
        # THEN check "Poor". Otherwise, "Poor" will also match "Very Poor"

        if re.search(veryPoorRePattern, comment):
            quality = 2
        elif re.search(poorRePattern, comment):
            quality = 1
        else:
            quality = 0

        nPointsRePattern = "Only (\d) points"

        if re.search(nPointsRePattern, comment):
            n = re.search(nPointsRePattern, comment).group(1)
            three_points = n
        else:
            three_points = 0

        # Now we build the dictionary which is the row that is added to the DataFrame
        newRow = {
                "id": f"{processed_columns[0].replace('/','')}{processed_columns[1].replace(':','')}",
                "date": f"{processed_columns[0].replace('/','-')}T{processed_columns[1]}",
                "pa": processed_columns[2],
                "width": processed_columns[3],
                "linear_speed": processed_columns[4],
                "2nd_order_initial_speed": processed_columns[5],
                "2nd_order_final_speed": processed_columns[6],
                "2nd_order_20R_speed": processed_columns[7],
                "acceleration": processed_columns[8],
                "mass": processed_columns[9],
                "kinetic_energy": processed_columns[10],
                "MPA": processed_columns[11],
                "halo": halo,
                "seen_in": seen_in,
                "quality": quality,
                "three_points": three_points
                }

        rows.append(newRow)

    finalDataFrame = pd.DataFrame.from_records(rows)

    finalDataFrame.to_csv(LASCO_CME_DATABASE, index=False)

if __name__ == "__main__":
    parse_lasco_cme_catalogue()
