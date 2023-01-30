from astropy.time import Time
import numpy as np

PI = np.pi
DEG_TO_RAD = PI / 180

class InvalidBoundingBox(Exception):
    """
    To be raised when a bounding box is not valid
    """
    def __init__(self, LON_MIN: float, LAT_MIN: float, LON_MAX: float, LAT_MAX: float):
        # Set values
        self.LON_MIN = LON_MIN
        self.LAT_MIN = LAT_MIN
        self.LON_MAX = LON_MAX
        self.LAT_MAX = LAT_MAX

        self.message = f"Bounding box [(lonmin, lonmax) (latmin, latmax)] [({self.LON_MIN},{self.LON_MAX}), ({self.LAT_MIN}, {self.LAT_MAX})] is not valid"
        super().__init__(self.message)
        

class Harps:
    def __init__(self, T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM=None):
        self.T_REC = Time(T_REC, format="iso")
        self.LON_MIN = float(LON_MIN)
        self.LAT_MIN = float(LAT_MIN)
        self.LON_MAX = float(LON_MAX)
        self.LAT_MAX = float(LAT_MAX)
        self.HARPNUM = HARPNUM

        self.__check_input_coordinates()

        self.RAW_CENTER_POINT = self._calculateRawCentrePoint()
        self.CARTESIAN_CENTER_POINT = self._calculateCartesianCoords()
        self.POSITION_ANGLE = self._calculatePositionAngle()
        self.DISTANCE_TO_SUN_CENTRE = self._calculateDistanceToSunCentre()

    def __check_input_coordinates(self):
        if (self.LON_MIN > self.LON_MAX) or (self.LAT_MIN > self.LAT_MAX):
            raise InvalidBoundingBox(self.LON_MIN, self.LAT_MIN, self.LON_MAX, self.LAT_MAX)

    def _calculatePositionAngle(self) -> np.float64:

        position_angle = np.arctan2(self.CARTESIAN_CENTER_POINT[1], self.CARTESIAN_CENTER_POINT[0]) * 180 / np.pi

        if 0 <= position_angle <= 90:
            position_angle += 270
        elif 90 < position_angle <= 180:
            position_angle -= 90
        elif position_angle < 0:
            position_angle += 270

        return position_angle

    def _calculateCartesianCoords(self) -> np.ndarray:
        return np.array([
            np.cos(self.RAW_CENTER_POINT[1] * DEG_TO_RAD) * np.sin(self.RAW_CENTER_POINT[0] * DEG_TO_RAD),
            np.sin(self.RAW_CENTER_POINT[1] * DEG_TO_RAD)
            ])

    def _calculateRawCentrePoint(self) -> np.ndarray:
        return np.array([
            np.average([self.LON_MIN, self.LON_MAX]),
            np.average([self.LAT_MIN, self.LAT_MAX])
            ])

    def _calculateDistanceToSunCentre(self) -> np.float64:
        return np.sqrt(self.CARTESIAN_CENTER_POINT[0] ** 2 + self.CARTESIAN_CENTER_POINT[1] ** 2)
