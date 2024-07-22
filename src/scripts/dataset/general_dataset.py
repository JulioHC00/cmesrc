import sys

sys.path.append("/home/julio/cmesrc/")

from src.cmesrc.config import CMESRC_DB, GENERAL_DATASET

import sqlite3
import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import astropy.units as u
from astropy.time import Time
import astropy.time
from typing import List, Tuple, Dict, Union, NamedTuple, Optional
from astropy.units.quantity import Quantity
from tqdm import tqdm


class Finished(Exception):
    """
    Raised when the data processing is finished.
    """

    pass


class InvalidObservationPeriod(Exception):
    """
    Raised when an observation period is invalid.

    Parameters
    ----------
    message : str
        The error message.
    reason : str
        The reason for invalidity. Must be one of ['missing_images', 'unclear_cme_present', 'final_cme_association'].
    dimming_id : str, optional
        The ID of the dimming event, required if reason is 'unclear_cme_present'.
    flare_id : str, optional
        The ID of the flare event, required if reason is 'unclear_cme_present'.
    cme_id : str, optional
        The ID of the CME event.
    """

    def __init__(
        self,
        message: str,
        reason: str,
        dimming_id: str = None,
        flare_id: str = None,
        cme_id: str = None,
    ) -> None:
        valid_reasons = [
            "missing_images",
            "unclear_cme_present",
            "final_cme_association",
        ]

        if reason not in valid_reasons:
            raise ValueError(f"Invalid reason {reason}.")

        self.message = message
        self.reason = reason

        if self.reason == "unclear_cme_present":
            if (dimming_id is None and flare_id is None) or cme_id is None:
                raise ValueError(
                    "Must provide at least one of dimming_id, flare_id and a cme_id."
                )

            self.dimming_id = dimming_id
            self.flare_id = flare_id
            self.cme_id = cme_id

        if self.reason == "final_cme_association":
            if cme_id is None:
                raise ValueError("Must provide a cme_id.")

            self.cme_id = cme_id


class NoBBoxData(Exception):
    """
    Raised when no bounding box data is available.
    """

    pass


def create_temp_table() -> None:
    conn = sqlite3.connect(CMESRC_DB)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS NO_LIMB_BBOX")

    cur.execute(
        """
                CREATE TABLE NO_LIMB_BBOX AS
                SELECT * FROM PROCESSED_HARPS_BBOX
                WHERE LONDTMIN > -70 AND LONDTMAX < 70
                """
    )

    cur.executescript(
        """
                CREATE INDEX IF NOT EXISTS idx_no_limb_bbox_harpnum ON NO_LIMB_BBOX(harpnum);
                CREATE INDEX IF NOT EXISTS idx_no_limb_bbox_timestamp ON NO_LIMB_BBOX(timestamp);
                CREATE INDEX IF NOT EXISTS idx_no_limb_bbox_harpnum_timestamp ON NO_LIMB_BBOX(harpnum, timestamp);
                CREATE INDEX IF NOT EXISTS idx_no_limb_bbox_londtmin ON NO_LIMB_BBOX(latdtmin);
                CREATE INDEX IF NOT EXISTS idx_no_limb_bbox_londtmax ON NO_LIMB_BBOX(latdtmax);
                CREATE INDEX IF NOT EXISTS idx_fcha_harpnum ON FINAL_CME_HARP_ASSOCIATIONS(harpnum);
                CREATE INDEX IF NOT EXISTS idx_cmes_date ON CMES(cme_date);
                """
    )

    conn.commit()
    conn.close()


def create_indices() -> None:
    conn = sqlite3.connect(CMESRC_DB)
    cur = conn.cursor()

    cur.executescript(
        """
                CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_harpnum ON PROCESSED_HARPS_BBOX(harpnum);
                CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_timestamp ON PROCESSED_HARPS_BBOX(timestamp);
                CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_harpnum_timestamp ON PROCESSED_HARPS_BBOX(harpnum, timestamp);
                CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_londtmin ON PROCESSED_HARPS_BBOX(latdtmin);
                CREATE INDEX IF NOT EXISTS idx_processed_harps_bbox_londtmax ON PROCESSED_HARPS_BBOX(latdtmax);
                CREATE INDEX IF NOT EXISTS idx_fcha_harpnum ON FINAL_CME_HARP_ASSOCIATIONS(harpnum);
                CREATE INDEX IF NOT EXISTS idx_cmes_date ON CMES(cme_date);
                """
    )


class AcceptedRow(NamedTuple):
    harpnum: int
    lead_in_start: str
    lead_in_end: str
    obs_start: str
    obs_end: str
    pred_start: str
    pred_end: str
    prev_cme_id: Optional[int]
    prev_diff: Optional[float]
    n_cmes: int
    counts_1: int
    counts_2: int
    counts_3: int
    counts_4: int
    counts_5: int
    label: int
    diff: Optional[float]
    verification_level: Optional[int]
    cme_id: Optional[int]


class RejectedRow(NamedTuple):
    harpnum: int
    lead_in_start: str
    lead_in_end: str
    obs_start: str
    obs_end: str
    pred_start: str
    pred_end: str
    reason: str
    message: str


# Define exception to be raise when finished

try:
    profile
except NameError:
    profile = lambda x: x


class HarpsDatasetSlices:
    # @conditional_decorator(typechecked)
    @profile
    def __init__(
        self,
        harpnum: int,
        O: Quantity,
        S: Quantity,
        db_path: str = CMESRC_DB,
        strict: bool = False,
        table: str = "PROCESSED_HARPS_BBOX",
    ):
        """
        Initialize a HarpsDatasetSlices object to manage HARPS dataset slices for solar physics research.

        Parameters
        ----------
        harpnum : int
            The HARP number of the dataset to be analyzed.
        O : Quantity
            The length of the observation period. Must be an Astropy Quantity object with time units.
        S : Quantity
            The step size for moving the observation window. Must be an Astropy Quantity object with time units.
        db_path : str, optional
            The path to the database containing HARPS data. Default is `CMESRC_DB`.
        strict : bool, optional
            Whether to enforce strict conditions for the observation period. Default is False.
        table : str, optional
            The name of the database table to query for data. Default is "PROCESSED_HARPS_BBOX".

        Attributes
        ----------
        table : str
            The name of the database table to use.
        harpnum : int
            The HARP number of the dataset.
        O : float
            The length of the observation period in hours.
        S : float
            The step size in hours.
        strict : bool
            Whether to enforce strict conditions for the observation period.
        conn : sqlite3.Connection
            The SQLite database connection.
        cur : sqlite3.Cursor
            The SQLite database cursor for executing SQL queries.
        first_timestamp : datetime
            The first timestamp of the dataset.
        last_timestamp : datetime
            The last timestamp of the dataset.
        current_timestamp : datetime
            The current timestamp for slicing.
        lead_in_period : tuple
            The start and end times of the lead-in period.
        observation_period : tuple
            The start and end times of the observation period.
        prediction_period : tuple
            The start and end times of the prediction period.
        finished : bool
            Whether the dataset has been fully processed.
        """

        # Initialize basic attributes
        self.table = table
        self.harpnum = int(harpnum)
        self.O = O.to(u.hour).value
        self.S = S.to(u.hour).value
        self.strict = strict

        # Check validity of period lengths
        self.__check_period_lengths()

        # Initialize database connection and cursor
        self._initialize_db(db_path)

        # Fetch the first and last timestamps
        self.first_timestamp, self.last_timestamp = self.__get_timestamp_bounds()

        # Normalize the timestamps
        self._normalize_timestamps()

        # Get the real lifetime

        self.life_start, self.life_end = self._get_real_lifetime()

        # Initialize other attributes
        self.current_timestamp = self.first_timestamp
        self.lead_in_period = None
        self.observation_period = None
        self.prediction_period = None
        self.finished = False

        # Compute initial period bounds
        self._get_period_bounds()

    def _get_real_lifetime(self):
        """
        While first and last timestamps, which are related to the observation period, must be bounded to be within the -70 and 70 degrees, when it comes to knowing how many CMEs happened before of when the next CME will happen we don't need
        to bound them by these -70 to 70 requirement. So here we get the real lifetime of the region
        """

        # Fetch the first timestamp
        self.cur.execute(
            f"SELECT MIN(datetime(timestamp)) FROM PROCESSED_HARPS_BBOX WHERE harpnum = ?",
            (self.harpnum,),
        )
        first_str_timestamp = self.cur.fetchone()[0]

        # Fetch the last timestamp
        self.cur.execute(
            f"SELECT MAX(datetime(timestamp)) FROM PROCESSED_HARPS_BBOX WHERE harpnum = ?",
            (self.harpnum,),
        )
        last_str_timestamp = self.cur.fetchone()[0]

        # Check if either timestamp is None
        if first_str_timestamp is None or last_str_timestamp is None:
            raise NoBBoxData()

        # Convert to datetime objects
        first_timestamp = datetime.fromisoformat(first_str_timestamp)
        last_timestamp = datetime.fromisoformat(last_str_timestamp)

        return first_timestamp, last_timestamp

    def _initialize_db(self, db_path: str):
        """
        Initialize the database connection and cursor.
        """
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cur = self.conn.cursor()

    def _normalize_timestamps(self):
        """
        Normalize the first and last timestamps to be at half past the hour.
        """
        self.first_timestamp = self.first_timestamp.replace(minute=30, second=0)
        self.last_timestamp = self.last_timestamp.replace(minute=30, second=0)

    def __check_period_lengths(self) -> None:
        """
        Validate the lengths of the observation and step periods.

        This method checks if the lengths of the observation period (`O`) and the step size (`S`)
        are both positive and multiples of 1 hour. Raises a `ValueError` if any of these conditions
        are not met.

        Raises
        ------
        ValueError
            If the observation period or step size is either negative or not a multiple of 1 hour.

        Returns
        -------
        None
        """
        # Define a mapping between attribute names and their human-readable equivalents
        period_names = {"O": "Observation period", "S": "Step size"}

        # Validate each period length
        for period_value, period_name in zip([self.O, self.S], period_names.keys()):
            if period_value < 0:
                raise ValueError(f"{period_names[period_name]} must be positive.")

            if period_value % 1 != 0:
                raise ValueError(
                    f"{period_names[period_name]} must be a multiple of 1 hour."
                )

    def __get_timestamp_bounds(self) -> Tuple[astropy.time.Time, astropy.time.Time]:
        """
        Get the first and last timestamps for the given harpnum.

        This method fetches the earliest and latest timestamps for the harpnum from the database.
        It raises a specific error if no data is found for the given harpnum.

        Raises
        ------
        NoBBoxData
            If no bounding box data is available for the given harpnum.

        Returns
        -------
        Tuple[astropy.time.Time, astropy.time.Time]
            A tuple containing the first and last timestamps as astropy.time.Time objects.
        """
        # Fetch the first timestamp
        self.cur.execute(
            f"SELECT MIN(datetime(timestamp)) FROM {self.table} WHERE harpnum = ?",
            (self.harpnum,),
        )
        first_str_timestamp = self.cur.fetchone()[0]

        # Fetch the last timestamp
        self.cur.execute(
            f"SELECT MAX(datetime(timestamp)) FROM {self.table} WHERE harpnum = ?",
            (self.harpnum,),
        )
        last_str_timestamp = self.cur.fetchone()[0]

        # Check if either timestamp is None
        if first_str_timestamp is None or last_str_timestamp is None:
            raise NoBBoxData()

        # Convert to datetime objects
        first_timestamp = datetime.fromisoformat(first_str_timestamp)
        last_timestamp = datetime.fromisoformat(last_str_timestamp)

        return first_timestamp, last_timestamp

    def _get_period_bounds(self) -> None:
        """
        Compute and set the bounds for the lead-in, observation, and prediction periods.

        This method calculates the bounds for the lead-in, observation, and prediction periods based
        on the current timestamp, observation period length (`O`), first timestamp, and last timestamp.

        Sets the attributes `lead_in_period`, `observation_period`, and `prediction_period` with the
        calculated bounds.

        Returns
        -------
        None
        """
        # Pre-compute end of observation period based on current timestamp and observation length
        end_of_observation = self.current_timestamp + timedelta(hours=self.O)

        # Calculate bounds for each period
        # Because of the half an hour bounds I need a max() call here to ensure that the pred_end is not smaller than obs_end
        #        lead_in_bounds = (self.first_timestamp, self.current_timestamp)
        lead_in_bounds = (
            min(self.life_start, self.first_timestamp),
            self.current_timestamp,
        )
        observation_bounds = (self.current_timestamp, end_of_observation)
        prediction_bounds = (
            end_of_observation,
            max(self.life_end, self.last_timestamp),
        )
        #        prediction_bounds = (end_of_observation, self.last_timestamp)

        # Assign calculated bounds to class attributes
        self.lead_in_period = lead_in_bounds
        self.observation_period = observation_bounds
        self.prediction_period = prediction_bounds

    def _check_observation_period(self) -> None:
        """
        Validate the current observation period based on certain conditions.

        This method checks if the current observation period is valid by querying the database.
        It raises an `InvalidObservationPeriod` exception if the observation period is invalid
        based on one of two conditions:
        1. If `strict` is True, it checks whether a CME event is associated with a dimming or flare.
        2. Checks for a final CME association.

        Raises
        ------
        InvalidObservationPeriod
            If the observation period is invalid based on the criteria.

        Returns
        -------
        None
        """
        # Extract and format start and end dates of the observation period
        start_dt, end_dt = self.observation_period
        start = (start_dt - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (end_dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Check for CME events if 'strict' is True
        if self.strict:
            self._check_strict_condition(start, end)

        # Check for final CME association
        self._check_final_cme_association(start, end)

    def _check_strict_condition(self, start: str, end: str) -> None:
        """Check if region was spatiotemporally consistent with a CME and if so, whether any dimming or flares are associated."""
        query = """
        SELECT CHE.cme_id, CHE.flare_id, CHE.dimming_id FROM CMES_HARPS_EVENTS CHE
        JOIN CMES C ON CHE.cme_id = C.cme_id
        JOIN CMES_HARPS_SPATIALLY_CONSIST CHSC ON CHSC.harpnum = CHE.harpnum AND CHSC.cme_id = CHE.cme_id
        WHERE ((CHE.dimming_id NOT NULL) OR (CHE.flare_id NOT NULL))
        AND CHE.harpnum = ? 
        AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
        AND CHE.cme_id NOT IN (SELECT cme_id FROM FINAL_CME_HARP_ASSOCIATIONS WHERE harpnum = ?)
        """
        self.cur.execute(query, (self.harpnum, start, end, self.harpnum))
        results = self.cur.fetchall()
        if len(results) != 0:
            raise InvalidObservationPeriod(
                "Observation period has a CME with dimming or flare associated with it.",
                reason="unclear_cme_present",
                dimming_id=results[0][2],
                flare_id=results[0][1],
                cme_id=results[0][0],
            )

    def _check_final_cme_association(self, start: str, end: str) -> None:
        """Check for a final CME association."""
        query = """
        SELECT FCHA.cme_id FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
        JOIN CMES C ON C.cme_id = FCHA.cme_id
        WHERE FCHA.harpnum = ?
        AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
        """
        self.cur.execute(query, (self.harpnum, start, end))
        results = self.cur.fetchall()
        if len(results) != 0:
            raise InvalidObservationPeriod(
                "Observation period has a final CME association.",
                reason="final_cme_association",
                cme_id=results[0][0],
            )

    def _get_previous_cme(
        self,
    ) -> Union[Tuple[None, None, int], Tuple[int, float, int]]:
        """
        Get the previous CME information for the given harpnum.

        This method queries the database to find CME events that occurred during the lead-in period
        for the given harpnum. It returns information about the closest CME to the start of the observation
        period, as well as the total number of CME events during the lead-in period.

        Raises
        ------
        ValueError
            If no lead-in period is available.

        Returns
        -------
        Union[Tuple[None, None, int], Tuple[int, float, int]]
            A tuple containing:
            - The closest CME ID (or None if no CMEs).
            - The time difference between the start of the observation period and the closest CME (or None).
            - The total number of CMEs in the lead-in period.
        """
        # Validate lead-in period
        if self.lead_in_period is None:
            raise ValueError("No lead-in period.")

        # Prepare time bounds for query
        start = self.lead_in_period[0].strftime("%Y-%m-%d %H:%M:%S")
        end = self.lead_in_period[1].strftime("%Y-%m-%d %H:%M:%S")

        # Query database for CMEs during the lead-in period
        query = """
            SELECT FCHA.cme_id, C.cme_date, FCHA.verification_score FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
            JOIN CMES C ON C.cme_id = FCHA.cme_id
            WHERE FCHA.harpnum = ?
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
            ORDER BY datetime(C.cme_date) DESC
        """
        self.cur.execute(query, (self.harpnum, start, end))
        results = self.cur.fetchall()

        counts = {level: 0 for level in range(1, 6)}

        # Check if any CMEs were found
        if len(results) == 0:
            return (None, None, 0, counts)

        for _, _, level in results:
            counts[level] += 1

        # Extract closest CME and calculate time difference
        cme_id = int(results[0][0])
        cme_date = datetime.fromisoformat(results[0][1])
        diff = float((self.observation_period[0] - cme_date).total_seconds() / 3600)

        return (cme_id, diff, len(results), counts)

    def _get_label(
        self,
    ) -> Tuple[int, Union[float, None], Union[int, None], Union[int, None]]:
        """
        Get the label and relevant metadata for the current observation period.

        This method queries the database to find CME events that occurred during the prediction period
        for the given harpnum. It returns information about the CME closest to the end of the observation
        period, as well as its verification level.

        Returns
        -------
        Tuple[int, Union[float, None], Union[int, None], Union[int, None]]
            A tuple containing:
            - Binary label indicating if a CME occurred (1) or not (0).
            - Time until the closest CME in hours (or None if no CMEs).
            - Verification level of the closest CME (or None).
            - ID of the closest CME (or None).
        """
        # Prepare time bounds for query
        start = self.prediction_period[0].strftime("%Y-%m-%d %H:%M:%S")
        end = self.prediction_period[1].strftime("%Y-%m-%d %H:%M:%S")

        # Query database for CMEs during the prediction period
        query = """
            SELECT FCHA.cme_id, C.cme_date, FCHA.verification_score FROM FINAL_CME_HARP_ASSOCIATIONS FCHA
            JOIN CMES C ON C.cme_id = FCHA.cme_id
            WHERE FCHA.harpnum = ?
            AND datetime(C.cme_date) BETWEEN datetime(?) AND datetime(?)
            ORDER BY datetime(C.cme_date) ASC
        """
        self.cur.execute(query, (self.harpnum, start, end))
        results = self.cur.fetchall()

        # Check if any CMEs were found
        if len(results) == 0:
            return (0, None, None, None)

        # Extract closest CME and calculate time difference
        cme_id = int(results[0][0])
        cme_date = datetime.fromisoformat(results[0][1])
        diff = float((cme_date - self.observation_period[1]).total_seconds() / 3600)
        verification_level = int(results[0][2])

        return (1, diff, verification_level, cme_id)

    def get_current_row(self) -> Tuple[int, Union[AcceptedRow, RejectedRow]]:
        """
        Retrieve the current row data for the dataset.

        Returns
        -------
        Tuple[int, Union[accepted_row, rejected_row]]
            A tuple containing a flag (1 for accepted row, 0 for rejected) and the row data.
        """

        # Validate the observation period
        try:
            self._check_observation_period()
        except InvalidObservationPeriod as e:
            # Build and return the rejected row
            return (
                0,  # Flag for a rejected row
                (
                    self.harpnum,
                    self.lead_in_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                    self.lead_in_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                    self.observation_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                    self.observation_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                    self.prediction_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                    self.prediction_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                    e.reason,
                    e.message,
                ),
            )

        # Get previous CME information and number of CMEs in the lead-in period
        prev_cme_id, prev_diff, n_cmes, counts = self._get_previous_cme()

        # Get the label for the observation period
        label, diff, verification_level, cme_id = self._get_label()

        # Build and return the accepted row
        return (
            1,  # Flag for an accepted row
            (
                self.harpnum,
                self.lead_in_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                self.lead_in_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                self.observation_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                self.observation_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                self.prediction_period[0].strftime("%Y-%m-%d %H:%M:%S"),
                self.prediction_period[1].strftime("%Y-%m-%d %H:%M:%S"),
                prev_cme_id,
                prev_diff,
                n_cmes,
                counts[1],
                counts[2],
                counts[3],
                counts[4],
                counts[5],
                label,
                diff,
                verification_level,
                cme_id,
            ),
        )

    def step(self) -> None:
        """
        Step the current timestamp forward by S.

        Returns
        -------
        None
        """

        # Check we haven't reached the end of the dataset
        if self.finished:
            raise Finished("Dataset has been fully processed.")

        next_observation_end = self.observation_period[1] + timedelta(hours=self.S)

        if next_observation_end > self.last_timestamp:
            self.finished = True
            return

        self.current_timestamp += timedelta(hours=self.S)
        self._get_period_bounds()


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


def get_harpnum_list(table: str = "NO_LIMB_BBOX") -> List[int]:
    conn = sqlite3.connect(CMESRC_DB)
    cur = conn.cursor()

    cur.execute(f"SELECT DISTINCT harpnum FROM {table}")

    harpnum_list = [int(x[0]) for x in cur.fetchall()]

    conn.close()

    return harpnum_list


def get_all_rows(
    O: Quantity, S: Quantity, strict: bool = False, table: str = "PROCESSED_HARPS_BBOX"
) -> Tuple[List[AcceptedRow], List[RejectedRow]]:
    """
    Generate all rows for a set of HARPs datasets.

    Parameters
    ----------
    O : Quantity
        The observation period length. Astropy units required.
    S : Quantity
        The step size. Astropy units required.
    strict : bool, optional
        Whether to enforce strict criteria for observation periods. Default is False.
    table : str, optional
        Name of the table in the database. Default is "PROCESSED_HARPS_BBOX".

    Returns
    -------
    Tuple[List[AcceptedRow], List[RejectedRow]]
        A tuple containing two lists: one for accepted rows and one for rejected rows.

    """

    # Get the list of HARPs numbers
    harpnum_list = get_harpnum_list()

    # Initialize lists for accepted and rejected rows
    accepted_rows = []
    rejected_rows = []

    # Loop through each HARPs number
    for harpnum in tqdm(harpnum_list):
        try:
            # Initialize the dataset
            dataset = HarpsDatasetSlices(harpnum, O, S, strict=strict, table=table)
            first_row = dataset.get_current_row()

        except NoBBoxData:
            # Handle cases with no bounding box data
            first_row = (
                0,
                (
                    harpnum,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "no_bbox_data",
                    "No BBox data for this harpnum.",
                ),
            )
            rejected_rows.append(first_row)
            continue

        # Add the first row to the appropriate list
        accepted_rows.append(first_row) if first_row[0] == 1 else rejected_rows.append(
            first_row
        )

        # Step through the dataset and collect rows
        dataset.step()
        while not dataset.finished:
            row = dataset.get_current_row()
            accepted_rows.append(row) if row[0] == 1 else rejected_rows.append(row)
            dataset.step()

    return accepted_rows, rejected_rows


def write_into_database(
    accepted_rows: List[AcceptedRow],
    rejected_rows: List[RejectedRow],
    db_path: str = GENERAL_DATASET,
    main_database=CMESRC_DB,
) -> None:
    """
    Write the accepted and rejected rows into the specified SQLite database.

    Parameters
    ----------
    accepted_rows : List[AcceptedRow]
        List of accepted rows to be inserted into the GENERAL_DATASET table.
    rejected_rows : List[RejectedRow]
        List of rejected rows to be inserted into the GENERAL_DATASET_REJECTED table.
    db_path : str, optional
        The path to the SQLite database. Default is CMESRC_DB.

    Returns
    -------
    None
    """

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    if main_database != db_path:
        conn.execute("ATTACH DATABASE ? AS CMESRCV3", (main_database,))

        # Create the HARPS table, copying from the main database

        cur.execute("DROP TABLE IF EXISTS main.HARPS")

        cur.execute(
            """
            CREATE TABLE main.HARPS AS
            SELECT * FROM CMESRCV3.HARPS
        """
        )

        # Same with CMEs table

        cur.execute("DROP TABLE IF EXISTS main.CMES")

        cur.execute(
            """
            CREATE TABLE main.CMES AS
            SELECT * FROM CMESRCV3.CMES
        """
        )

        cur.execute("DROP TABLE IF EXISTS main.FINAL_CME_HARP_ASSOCIATIONS")

        cur.execute(
            """
            CREATE TABLE main.FINAL_CME_HARP_ASSOCIATIONS AS
            SELECT * FROM CMESRCV3.FINAL_CME_HARP_ASSOCIATIONS
            """
        )

        # Same with the HARPS_KEYWORDS table

        cur.execute("DROP TABLE IF EXISTS main.HARPS_KEYWORDS")

        cur.execute(
            """
        CREATE TABLE main.HARPS_KEYWORDS AS
        SELECT * FROM CMESRCV3.HARPS_KEYWORDS
        """
        )

        # Detach the main database

        conn.commit()

        conn.execute("DETACH DATABASE CMESRCV3")

        conn.commit()

    # Create the accepted_rows table
    cur.execute("DROP TABLE IF EXISTS GENERAL_DATASET")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS GENERAL_DATASET (
            slice_id INTEGER PRIMARY KEY AUTOINCREMENT,
            harpnum INT NOT NULL,
            lead_in_start TEXT NOT NULL,
            lead_in_end TEXT NOT NULL,
            obs_start TEXT NOT NULL,
            obs_end TEXT NOT NULL,
            pred_start TEXT NOT NULL,
            pred_end TEXT NOT NULL,
            prev_cme_id INT,
            prev_cme_diff REAL,
            n_cmes_before INTEGER NOT NULL,
            n_cmes_before_1 INTEGER NOT NULL,
            n_cmes_before_2 INTEGER NOT NULL,
            n_cmes_before_3 INTEGER NOT NULL,
            n_cmes_before_4 INTEGER NOT NULL,
            n_cmes_before_5 INTEGER NOT NULL,
            label INTEGER NOT NULL,
            cme_diff REAL,
            verification_level INTEGER,
            cme_id INT
        )
    """
    )

    # Create the rejected_rows table
    cur.execute("DROP TABLE IF EXISTS GENERAL_DATASET_REJECTED")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS GENERAL_DATASET_REJECTED (
            slice_id INTEGER PRIMARY KEY AUTOINCREMENT,
            harpnum INT NOT NULL,
            lead_in_start TEXT,
            lead_in_end TEXT,
            obs_start TEXT,
            obs_end TEXT,
            pred_start TEXT,
            pred_end TEXT,
            reason TEXT NOT NULL,
            message TEXT NOT NULL
        )
    """
    )
    # Insert the rows into the tables
    for row in tqdm(accepted_rows):
        # Make double sure harpnum is int
        # Also prev_cme_id and cme_id

        nrow = list(row[1])
        nrow[0] = int(nrow[0])
        nrow = tuple(nrow)

        try:
            cur.execute(
                """
                INSERT INTO GENERAL_DATASET
                (harpnum, lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end, prev_cme_id, prev_cme_diff, n_cmes_before, n_cmes_before_1, n_cmes_before_2, n_cmes_before_3, n_cmes_before_4, n_cmes_before_5, label, cme_diff, verification_level, cme_id)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                nrow,
            )
        # Except foreign key constraint violations
        except sqlite3.OperationalError as e:
            print(nrow)
            raise e

    for row in tqdm(rejected_rows):
        nrow = list(row[1])
        nrow[0] = int(nrow[0])
        nrow = tuple(nrow)

        try:
            cur.execute(
                """
                INSERT INTO GENERAL_DATASET_REJECTED
                (harpnum, lead_in_start, lead_in_end, obs_start, obs_end, pred_start, pred_end, reason, message)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                nrow,
            )
        # Except foreign key constraint violations
        except sqlite3.OperationalError as e:
            raise e

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_indices()
    create_temp_table()
    accepted_rows, rejected_rows = get_all_rows(
        24 * u.hour, 1 * u.hour, strict=True, table="NO_LIMB_BBOX"
    )
    write_into_database(accepted_rows, rejected_rows)

