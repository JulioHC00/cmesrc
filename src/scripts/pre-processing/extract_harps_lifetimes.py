"""
Uses the SWAN files to extract a list of all HARPS regions along with the time of their first appearance in HARPS data and last appearance
"""

import os
import pandas as pd
from src.cmesrc.config import SWAN_DATA_DIR, HARPS_LIFETIME_DATABSE

def generate_HARPS_lifetime_database():
    harpsDict = dict()

    for directoryName, subdirectoryName, fileList in os.walk(SWAN_DATA_DIR):
        for fileName in fileList:
            harpsNum = str(fileName.split(".")[0]) # To remove the file extension 

            # Now to get the first and last timestamp
            timeStamps = pd.read_csv(os.path.join(directoryName, fileName), sep="\t", usecols=["Timestamp"]).to_numpy()

            firstTimeStamp = str(timeStamps[0][0])
            lastTimeStamp = str(timeStamps[-1][0])

            harpsDict[harpsNum] = {
                    "start": firstTimeStamp,
                    "end": lastTimeStamp
                    }

    harpsLifetimeDataFrame = pd.DataFrame.from_dict(harpsDict, orient='index')

    harpsLifetimeDataFrame.to_csv(HARPS_LIFETIME_DATABSE, index_label="harpsnum")


if __name__ == "__main__":
    generate_HARPS_lifetime_database()
