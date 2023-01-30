from astropy.time import Time
from astropy.coordinates import SkyCoord
import astropy.units as u
from sunpy.coordinates import HeliographicStonyhurst, propagate_with_solar_surface
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
        

class BaseHarps:
    def __init__(self, T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM=None):
        self.T_REC = Time(T_REC, format="iso")
        self.LON_MIN = float(LON_MIN)
        self.LAT_MIN = float(LAT_MIN)
        self.LON_MAX = float(LON_MAX)
        self.LAT_MAX = float(LAT_MAX)
        self.FRAME = HeliographicStonyhurst(obstime=self.T_REC)
        self.HARPNUM = HARPNUM
        self.__ROTATED = None

        self.__check_input_coordinates()

        self.RAW_CENTER_POINT = self._calculateRawCentrePoint()
        self.CARTESIAN_CENTER_POINT = self._calculateCartesianCoords()
        self.POSITION_ANGLE = self._calculatePositionAngle()
        self.DISTANCE_TO_SUN_CENTRE = self._calculateDistanceToSunCentre()

    def __check_input_coordinates(self):
        if (self.LON_MIN > self.LON_MAX) or (self.LAT_MIN > self.LAT_MAX):
            raise InvalidBoundingBox(self.LON_MIN, self.LAT_MIN, self.LON_MAX, self.LAT_MAX)

    def is_rotated(self):
        return self.__ROTATED

    def _set_rotation(self, rotation):
        self.__ROTATED = rotation

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

    def get_raw_bounding_box(self):
        return [[self.LON_MIN, self.LAT_MIN], [self.LON_MAX, self.LAT_MAX]]

    def get_skycoord_bounding_box(self):
        return SkyCoord([
            [self.LON_MIN * u.deg, self.LAT_MIN * u.deg],
            [self.LON_MAX * u.deg, self.LAT_MAX * u.deg]
            ],
                        frame=self.FRAME
                        )

class RotatedHarps(BaseHarps):
    def __init__(self, T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM=None):
        super().__init__(T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM)
        self._set_rotation(True)

class Harps(BaseHarps):
    def __init__(self, T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM=None):
        super().__init__(T_REC, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, HARPNUM)
        self._set_rotation(False)

    def rotate_coords(self, date):
        original_bbox = self.get_skycoord_bounding_box()
        new_frame = HeliographicStonyhurst(obstime = date)

        with propagate_with_solar_surface():
            new_bbox = original_bbox.transform_to(new_frame)

        new_coords = new_bbox.to_string()

        new_lon_min, new_lat_min = (float(coord) for coord in new_coords[0].split())
        new_lon_max, new_lat_max = (float(coord) for coord in new_coords[1].split())

        return RotatedHarps(
                T_REC = date,
                LON_MIN = new_lon_min,
                LAT_MIN = new_lat_min,
                LON_MAX = new_lon_max,
                LAT_MAX = new_lat_max,
                HARPNUM = self.HARPNUM
                )
