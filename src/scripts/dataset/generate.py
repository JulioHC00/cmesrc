import sys
sys.path.append('/home/julio/cmesrc/')

from src.cmesrc.config import CMESRC_DB

import sqlite3
import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import astropy.units as u
from astropy.time import Time
import astropy.time
from typing import List, Tuple, Dict, Union
from astropy.units.quantity import Quantity
from tqdm import tqdm

class Finished(Exception):
    pass

class InvalidObservationPeriod(Exception):
    def __init__(self, message: str, reason: str, dimming_id: str = None, flare_id: str = None, cme_id: str = None) -> None:
        self.message = message

        if reason not in ['missing_images', 'unclear_cme_present', 'final_cme_association']:
            raise ValueError(f"Invalid reason {reason}.")

        self.reason = reason

        if self.reason == "unclear_cme_present":
            if (dimming_id is None and flare_id is None) or cme_id is None:
                raise ValueError("Must provide at least one of dimming_id, flare_id and a cme_id.")
            else:
                self.dimming_id = dimming_id
                self.flare_id = flare_id
                self.cme_id = cme_id
        
        if self.reason == "final_cme_association":
            if cme_id is None:
                raise ValueError("Must provide a cme_id.")
            else:
                self.cme_id = cme_id

def create_hourly_table(force: bool = False) -> None:
    conn = sqlite3.connect(CMESRC_DB)
    cur = conn.cursor()

    if force:
        cur.execute("DROP TABLE IF EXISTS HOURLY_BBOX")
        cur.execute("DROP TABLE IF EXISTS HOURLY_PIXEL_BBOX")

    cur.execute("""
            CREATE TABLE IF NOT EXISTS HOURLY_BBOX AS 
            SELECT * FROM PROCESSED_HARPS_BBOX
            WHERE strftime("%M", timestamp) IN ("00", "12", "24")
            GROUP BY harpnum, strftime("%Y %m %d %H", timestamp)
                """)
    
    cur.execute("""
            CREATE TABLE IF NOT EXISTS HOURLY_PIXEL_BBOX AS
            SELECT PHPBB.* FROM PROCESSED_HARPS_PIXEL_BBOX PHPBB
            JOIN HOURLY_BBOX HBB
            ON PHPBB.harpnum = HBB.harpnum AND PHPBB.timestamp = HBB.timestamp
            WHERE HBB.LONDTMIN > -70 AND HBB.LONDTMAX < 70
            """)

    cur.executescript("""
                CREATE INDEX IF NOT EXISTS idx_hourly_bbox_harpnum ON HOURLY_BBOX(harpnum);
                CREATE INDEX IF NOT EXISTS idx_hourly_bbox_timestamp ON HOURLY_BBOX(timestamp);
                CREATE INDEX IF NOT EXISTS idx_hourly_bbox_harpnum_timestamp ON HOURLY_BBOX(harpnum, timestamp);
                CREATE INDEX IF NOT EXISTS idx_hourly_bbox_londtmin ON HOURLY_BBOX(latdtmin);
                CREATE INDEX IF NOT EXISTS idx_hourly_bbox_londtmax ON HOURLY_BBOX(latdtmax);
                CREATE INDEX IF NOT EXISTS idx_fcha_harpnum ON FINAL_CME_HARP_ASSOCIATIONS(harpnum);
                CREATE INDEX IF NOT EXISTS idx_cmes_date ON CMES(cme_date);
                """)

    conn.close()

# Accepted row is
# (
# lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end,
# prev_cme_id, prev_diff, label, diff, verification_level, cme_id, n_images
# )

# Rejected row is
# (
# lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end,
# reason
# )
accepted_row = Tuple[int, str, str, str, str, str, str, Union[int, None], Union[float, None], int, Union[float, None], Union[int, None], Union[int, None], int]
rejected_row = Tuple[int, str, str, str, str, str, str, str]

# Define exception to be raise when finished

try:
    profile
except NameError:
    profile = lambda x: x

class HarpsDatasetSlices():
    #@conditional_decorator(typechecked)
    @profile
    def __init__(self, harpnum: int, O: Quantity, P: Quantity, L: Quantity, S: Quantity, db_path: str = CMESRC_DB, strict: bool = False):
        """
        Create a HarpsDatasetSlices object.

        Parameters
        ----------  
        harpnum : int
            The harp number of the dataset.
        O : Quantity
            The observation period length. Astropy units required.
        P : Quantity
            The prediction period length. Astropy units required.
        L : Quantity
            The lead-in period length. Astropy units required.
        S : Quantity
            The step size. Astropy units required.
        """
        self.harpnum = int(harpnum)
        self.O = O.to(u.hour).value
        self.P = P.to(u.hour).value
        self.L = L.to(u.hour).value
        self.S = S.to(u.hour).value

        self.strict = strict

        self.__check_period_lengths()

        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cur = self.conn.cursor()

        self.first_timestamp = self.__get_first_timestamp()
        self.last_timestamp = self.__get_last_timestamp()

        # Need to force these timestamps to be at o'clock
        self.first_timestamp = self.first_timestamp.replace(minute=30, second=0)
        self.last_timestamp = self.last_timestamp.replace(minute=30, second=0)

        self.current_timestamp = self.first_timestamp

        self.lead_in_period = None
        self.observation_period = None
        self.prediction_period = None
        self._get_period_bounds()

        self.finished = False

    #@conditional_decorator(typechecked)
    @profile
    def __check_period_lengths(self) -> None:
        """
        Check if the period lengths are valid. Return the original values if they are valid.
        Raise an error if they are not valid.
        """

        names = {
            'O': "Observation period",
            'P': "Prediction period",
            'L': "Lead-in period",
            'S': "Step size"
        }

        for period, name in zip([self.O, self.P, self.L, self.S], names.keys()):
            if period < 0:
                raise ValueError(f"{names[name]} must be positive.")
            if (period * 60) % 12 != 0 * u.minute:
                raise ValueError(
                    f"{names[name]} must be a multiple of 12 minutes.")
            else:
                continue

        return None

    #@conditional_decorator(typechecked)
    @profile
    def __get_first_timestamp(self) -> astropy.time.Time:
        """
        Get the first timestamp for the harpnum.
        """

        self.cur.execute(
            "SELECT MIN(datetime(timestamp)) FROM HOURLY_BBOX WHERE harpnum = ?", (self.harpnum,))

        str_timestamp = self.cur.fetchone()[0]

        if str_timestamp is None:
            raise ValueError(f"No data for harpnum {self.harpnum}")

        return datetime.fromisoformat(str_timestamp)

    #@conditional_decorator(typechecked)
    @profile
    def __get_last_timestamp(self) -> astropy.time.Time:
        """
        Get the last timestamp for the harpnum.
        """

        self.cur.execute(
            "SELECT MAX(datetime(timestamp)) FROM HOURLY_BBOX WHERE harpnum = ?", (self.harpnum,))

        str_timestamp = self.cur.fetchone()[0]

        if str_timestamp is None:
            raise ValueError(f"No data for harpnum {self.harpnum}")

        return datetime.fromisoformat(str_timestamp)

    @profile
    def _get_period_bounds(self) -> None:

        # Pre-compute intermediate results
        self.current_minus_L = self.current_timestamp - timedelta(hours=self.L)
        self.current_plus_O = self.current_timestamp + timedelta(hours=self.O)
        self.current_plus_O_plus_P = self.current_plus_O + timedelta(hours=self.P)

        # Use temporary variables to hold period data
        if self.current_minus_L < self.first_timestamp:
            temp_lead_in = (self.first_timestamp, self.current_timestamp)
        else:
            temp_lead_in = (self.current_minus_L, self.current_timestamp)

        temp_observation = (self.current_timestamp, self.current_plus_O)
        temp_prediction = (self.current_plus_O, self.current_plus_O_plus_P)

        # Assign period variables once
        self.lead_in_period = temp_lead_in
        self.observation_period = temp_observation
        self.prediction_period = temp_prediction

        return None

    #@conditional_decorator(typechecked)
#    @profile
#    def _get_period_bounds(self) -> None:
#
#        # Reset the period bounds
#        self.lead_in_period = None
#        self.observation_period = None
#        self.prediction_period = None
#
#        if self.current_timestamp - self.L < self.first_timestamp:
#            self.lead_in_period = (self.first_timestamp,
#                                   self.current_timestamp)
#
#
#        self.lead_in_period = (self.current_timestamp -
#                               self.L, self.current_timestamp)
#        self.observation_period = (
#            self.current_timestamp, self.current_timestamp + self.O)
#        self.prediction_period = (
#            self.current_timestamp + self.O, self.current_timestamp + self.O + self.P)
#        return None

    #@conditional_decorator(typechecked)
    @profile
    def _check_observation_period(self) -> int:
        # It is assumed that everytime the current timestamp is updated,
        # the period bounds are also updated.

        start_dt, end_dt = self.observation_period

        # Format to yyyy-mm-dd hh:mm:ss

        start = (start_dt - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (end_dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Need to count how many images there are in the observation period, with
        # LONDTMIN > -70 and LONDTMAX < 70

        self.cur.execute("""
        SELECT COUNT(*) FROM HOURLY_BBOX
        WHERE harpnum = ? 
        AND timestamp BETWEEN ? AND ?
        AND LONDTMIN > -70 AND LONDTMAX < 70
        """, (self.harpnum, start, end))

#        AND datetime(timestamp) BETWEEN datetime(?) AND datetime(?)
        count = int(self.cur.fetchone()[0])

        # The number of images should be equal to O (with O in hours)
        # If missing more than 1 images, then the observation period is invalid

        if count < int(self.O) - 1 :
            raise InvalidObservationPeriod(f"Observation period is missing {int(self.O) - count} images.",
                                           reason='missing_images')

        # If it's missing one check it's not the last one
        if count < int(self.O):
            # Need to check the last timestamp is not more than one hour away from the end of the observation period
            self.cur.execute("""
            SELECT MAX(timestamp) FROM HOURLY_BBOX
            WHERE harpnum = ?
            AND timestamp BETWEEN ? AND ?
            AND LONDTMIN > -70 AND LONDTMAX < 70
            """, (self.harpnum, start, end))

            last_timestamp = datetime.fromisoformat(self.cur.fetchone()[0])

            if (end_dt - last_timestamp).total_seconds() / 3600 > 1:
                raise InvalidObservationPeriod("Observation period is missing the last image.",
                                               reason='missing_images')


        # If strict is True, then we require that during the observation period, the region
        # wasn't spatially consistent with a CME AND had a dimming or a flare or both

        if self.strict:
            # See if region was present at CME and if so if any dimming or flares are associated with that pressence

            query = """     
            SELECT CHE.cme_id, CHE.flare_id, CHE.dimming_id FROM CMES_HARPS_EVENTS CHE
            JOIN CMES C
            ON CHE.cme_id = C.cme_id
            WHERE ((CHE.dimming_id NOT NULL) OR (CHE.flare_id NOT NULL))
            AND CHE.harpnum = ? 
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
            """

            self.cur.execute(query, (self.harpnum, start, end))

            results = self.cur.fetchall()

            # If length is 0 we're good to keep going, the observation period is clean
            if len(results) == 0:
                pass
            else:
                # Otherwise we must invalidate it
                raise InvalidObservationPeriod(f"Observation period has a CME with dimming or flare associated with it.",
                                               reason='unclear_cme_present',
                                               dimming_id=results[0][2],
                                               flare_id=results[0][1],
                                               cme_id=results[0][0])
        
        # Given the strict checks, we now do the normal one. There must not be a final CME association 

        query = """
            SELECT FCHA.cme_id FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
            JOIN CMES C
            ON C.cme_id = FCHA.cme_id
            WHERE FCHA.harpnum = ?
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
        """

        self.cur.execute(query, (self.harpnum, start, end))

        results = self.cur.fetchall()

        # Again results length of 0 means we're good to go

        if len(results) == 0:
            pass
        else:
            # Otherwise we must invalidate it
            raise InvalidObservationPeriod(f"Observation period has a final CME association.",
                                           reason='final_cme_association',
                                           cme_id=results[0][0])
        
        return count
    
    #@conditional_decorator(typechecked)
    @profile
    def _get_previous_cme(self) -> Union[Tuple[None,None], Tuple[int, float]]:
        """
        Get the previous CME for the harpnum.
        """
        # Check if there's a lead-in period. If not, raise an error
        if self.lead_in_period is None:
            raise ValueError("No lead-in period.")

        # We use the lead-in period to get the previous CME
        start = self.lead_in_period[0].strftime("%Y-%m-%d %H:%M:%S")
        end = self.lead_in_period[1].strftime("%Y-%m-%d %H:%M:%S")

        # Query closest CME to observation period
        query = """
            SELECT FCHA.cme_id, C.cme_date FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
            JOIN CMES C
            ON C.cme_id = FCHA.cme_id
            WHERE FCHA.harpnum = ?
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
            ORDER BY datetime(C.cme_date) DESC
        """

        self.cur.execute(query, (self.harpnum, start, end))

        results = self.cur.fetchall()

        # If length is 0, then there's no previous CME
        if len(results) == 0:
            return (None,None)
        else:
            # Otherwise, return cme_id and diff with start of observation period

            cme_id = int(results[0][0])
            cme_date = datetime.fromisoformat(results[0][1])
            diff = float((self.observation_period[0] - cme_date).total_seconds() / 3600)

            return cme_id, diff

    #@conditional_decorator(typechecked)
    @profile
    def _get_label(self) -> Tuple[int, Union[float, None], Union[int, None], Union[int, None]]: # binary label, hours until CME, verification level, cme_id
        """
        Get the label for the observation period.
        """

        start = self.prediction_period[0].strftime("%Y-%m-%d %H:%M:%S")
        end = self.prediction_period[1].strftime("%Y-%m-%d %H:%M:%S")

        query = """
            SELECT FCHA.cme_id, C.cme_date, FCHA.verification_score FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
            JOIN CMES C
            ON C.cme_id = FCHA.cme_id
            WHERE FCHA.harpnum = ?
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
            ORDER BY datetime(C.cme_date) ASC
        """

        self.cur.execute(query, (self.harpnum, start, end))

        results = self.cur.fetchall()

        # If length is 0, then there's no CME in the prediction period
        if len(results) == 0:
            return (0, None, None, None)
        else:
            # Otherwise, return cme_id and diff with end of observation period

            cme_id = int(results[0][0])
            cme_date = datetime.fromisoformat(results[0][1])
            diff = float((cme_date - self.observation_period[1]).total_seconds() / 3600)
            verification_level = int(results[0][2])

            return (1, diff, verification_level, cme_id)
    
    #@conditional_decorator(typechecked)
    @profile
    def get_current_row(self) -> Tuple[int, Union[accepted_row, rejected_row]]:

        # Next we have to make sure the observation period is valid
        try:
            n_images = self._check_observation_period()
        except InvalidObservationPeriod as e:
            # If it's not valid, then we return the rejected row
            row = (0, # Because it's a rejected row
                   (self.harpnum,
                    self.lead_in_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                    self.lead_in_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                    self.observation_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                    self.observation_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                    self.prediction_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                    self.prediction_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                    e.reason,
                    e.message))
            return row

        # Next we have to get the previous CME
        prev_cme_id, prev_diff = self._get_previous_cme()

        # Next we have to get the label
        label, diff, verification_level, cme_id = self._get_label()

        # The row is then
        # (lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end, prev_cme_id, prev_diff, label, diff, verification_level, cme_id, n_images)

        row = (1, # Because it's an accepted row
               (self.harpnum,
                self.lead_in_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                self.lead_in_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                self.observation_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                self.observation_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                self.prediction_period[0].strftime('%Y-%m-%d %H:%M:%S'),
                self.prediction_period[1].strftime('%Y-%m-%d %H:%M:%S'),
                prev_cme_id,
                prev_diff,
                label,
                diff,
                verification_level,
                cme_id,
                n_images))
        
        return row
    
    #@conditional_decorator(typechecked)
    @profile
    def step(self) -> None:
        """
        Step the current timestamp forward by S.
        """

        if self.prediction_period[1] + timedelta(hours=self.S) > self.last_timestamp:
            self.finished = True
            return None

        self.current_timestamp += timedelta(hours=self.S)

        self._get_period_bounds()

        return None

@profile
def test_run() -> None:
    O = 12 * u.hour 
    P = 24 * u.hour
    L = 24 * u.hour
    S = 1 * u.hour
    test = HarpsDatasetSlices(8, O, P, L, S)

    first_row = test.get_current_row()
    test.step()

    while not test.finished:
        test.step()
        if test.finished:
            break
        next_row = test.get_current_row()
        print(next_row)

def get_harpnum_list() -> List[int]:
    conn = sqlite3.connect(CMESRC_DB)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT harpnum FROM HOURLY_BBOX")

    harpnum_list = [int(x[0]) for x in cur.fetchall()]

    conn.close()

    return harpnum_list

def get_all_rows(O: Quantity, P: Quantity, L: Quantity, S: Quantity, strict: bool = False) -> Tuple[List[accepted_row], List[rejected_row]]:
    harpnum_list = get_harpnum_list()

    accepted_rows = []
    rejected_rows = []

    for harpnum in tqdm(harpnum_list):
        dataset = HarpsDatasetSlices(harpnum, O, P, L, S, strict=strict)
        firs_row = dataset.get_current_row()
        _ = accepted_rows.append(firs_row) if firs_row[0] == 1 else rejected_rows.append(firs_row)
        dataset.step()
        while not dataset.finished:
            row = dataset.get_current_row()
            _ = accepted_rows.append(row) if row[0] == 1 else rejected_rows.append(row)
            dataset.step()
            if dataset.finished:
                break
    
    return accepted_rows, rejected_rows

def write_into_database(accepted_rows: List[accepted_row], rejected_rows: List[rejected_row], db_path: str = CMESRC_DB) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # First we need to create the accepted_rows table

    cur.execute("DROP TABLE IF EXISTS HARPS_DATASET_SLICES")

    cur.execute("""
                CREATE TABLE IF NOT EXISTS HARPS_DATASET_SLICES (
                    slice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    harpnum INTEGER NOT NULL REFERENCES HARPS(harpnum),
                    lead_in_start TEXT NOT NULL,
                    lead_in_end TEXT NOT NULL,
                    obs_start TEXT NOT NULL,
                    obs_end TEXT NOT NULL,
                    pred_start TEXT NOT NULL,
                    pred_end TEXT NOT NULL,
                    prev_cme_id INTEGER REFERENCES CMES(cme_id),
                    prev_cme_diff REAL,
                    label INTEGER NOT NULL,
                    cme_diff REAL,
                    verification_level INTEGER,
                    cme_id INTEGER REFERENCES CMES(cme_id),
                    n_images INTEGER NOT NULL
                    )
                """
                )
    
    # Now the rejected_rows table

    cur.execute("DROP TABLE IF EXISTS HARPS_DATASET_REJECTED_SLICES")

    cur.execute("""
                CREATE TABLE IF NOT EXISTS HARPS_DATASET_REJECTED_SLICES (
                    slice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    harpnum INTEGER NOT NULL REFERENCES HARPS(harpnum),
                    lead_in_start TEXT NOT NULL,
                    lead_in_end TEXT NOT NULL,
                    obs_start TEXT NOT NULL,
                    obs_end TEXT NOT NULL,
                    pred_start TEXT NOT NULL,
                    pred_end TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    message TEXT NOT NULL
                    )
                """
                ) 
        
    # Now we insert the rows
    for row in tqdm(accepted_rows):
        cur.execute("""
                    INSERT INTO HARPS_DATASET_SLICES
                    (harpnum, lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end, prev_cme_id, prev_cme_diff, label, cme_diff, verification_level, cme_id, n_images)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row[1])
    
    for row in tqdm(rejected_rows):
        cur.execute("""
                    INSERT INTO HARPS_DATASET_REJECTED_SLICES
                    (harpnum, lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end, reason, message)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row[1])
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_hourly_table(force=False)
    accepted_rows, rejected_rows = get_all_rows(24 * u.hour, 24 * u.hour, 12 * u.hour, 1 * u.hour, strict=True)
    write_into_database(accepted_rows, rejected_rows)