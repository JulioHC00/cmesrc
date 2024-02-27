from src.cmesrc.exception_classes import InvalidBoundingBox
from sunpy.coordinates import HeliographicStonyhurst, propagate_with_solar_surface
from astropy.time import Time
from astropy.units import Quantity, Unit
from astropy.coordinates import SkyCoord
import numpy as np

DEG_TO_RAD = np.pi / 180


class Point:
    def __init__(self, date, lon: float, lat: float, units: str = "deg"):
        self.DATE = Time(date, format="iso")
        self.LON = lon
        self.LAT = lat
        self.UNITS = units
        self.FRAME = HeliographicStonyhurst(obstime=self.DATE)

    def __eq__(self, other):
        if type(other) is Point:
            if np.all(
                [
                    self.LON == other.LON,
                    self.LAT == other.LAT,
                    self.DATE == other.DATE,
                    self.UNITS == other.UNITS,
                    self.FRAME == other.FRAME,
                ]
            ):
                return True
            else:
                return False

    def __add__(self, other):
        if type(other) is Point:
            if self.FRAME != other.FRAME:
                raise ValueError("Can't add points with different dates or frames.")

            if other.UNITS != self.UNITS:
                otherPoint = other.change_units(self.UNITS)

            return Point(
                date=self.DATE,
                lon=self.LON + other.LON,
                lat=self.LAT + other.LAT,
                units=self.UNITS,
            )
        else:
            raise TypeError(
                f"unsupported operand type(s) for +: 'Point' and '{type(other)}'"
            )

    def __truediv__(self, other):
        if (type(other) is float) or (type(other) is int):
            return Point(
                date=self.DATE,
                lon=self.LON / other,
                lat=self.LAT / other,
                units=self.UNITS,
            )
        else:
            raise TypeError(
                f"unsupported operand type(s) for /: 'Point' and '{type(other)}'"
            )

    def change_units(self, new_units: str, inplace=False):
        new_lon = Quantity(self.LON, unit=self.UNITS).to(Unit(new_units))
        new_lat = Quantity(self.LAT, unit=self.UNITS).to(Unit(new_units))

        if inplace:
            self.LON = new_lon.value
            self.LAT = new_lat.value
            self.UNITS = new_units
            return self

        return Point(
            date=self.DATE, lon=new_lon.value, lat=new_lat.value, units=new_units
        )

    def get_skycoord(self) -> SkyCoord:
        return SkyCoord(self.LON, self.LAT, unit=self.UNITS, frame=self.FRAME)

    def get_raw_coords(self) -> list:
        return [self.LON, self.LAT]

    def get_cartesian_coords(self) -> list:
        return [
            np.cos(self.LAT * DEG_TO_RAD) * np.sin(self.LON * DEG_TO_RAD),
            np.sin(self.LAT * DEG_TO_RAD),
        ]

    def get_position_angle(self) -> float:
        cartesian_coords = self.get_cartesian_coords()

        position_angle = (
            np.arctan2(cartesian_coords[1], cartesian_coords[0]) * 180 / np.pi
        )

        if 0 <= position_angle <= 90:
            position_angle += 270
        elif 90 < position_angle <= 180:
            position_angle -= 90
        elif position_angle < 0:
            position_angle += 270

        return position_angle

    def get_distance_to_sun_centre(self) -> np.float64:
        cartesian_coords = self.get_cartesian_coords()
        return np.sqrt(cartesian_coords[0] ** 2 + cartesian_coords[1] ** 2)

    def rotate_coords(self, new_date, inplace: bool = False):
        current_coords = self.get_skycoord()
        current_frame = self.FRAME

        new_time = Time(new_date)
        new_frame = HeliographicStonyhurst(obstime=new_time)

        with propagate_with_solar_surface():
            new_coords = current_coords.transform_to(new_frame)

        new_lon, new_lat = [float(coord) for coord in new_coords.to_string().split()]

        if inplace:
            self.DATE = new_date
            self.LON = new_lon
            self.LAT = new_lat
            self.FRAME = new_frame
            return self

        return Point(date=new_date, lon=new_lon, lat=new_lat, units=self.UNITS)


class BoundingBox:
    def __init__(
        self,
        date,
        lon_min: float,
        lat_min: float,
        lon_max: float,
        lat_max: float,
        units: str = "deg",
    ):
        self.LOWER_LEFT = Point(date=date, lon=lon_min, lat=lat_min, units=units)

        self.UPPER_RIGHT = Point(date=date, lon=lon_max, lat=lat_max, units=units)

        self.CENTRE_POINT = self.get_centre_point()

        self.DATE = Time(date)
        self.UNITS = units
        self.FRAME = HeliographicStonyhurst(obstime=self.DATE)

        self.__check_input_coordinates()

    def __check_input_coordinates(self):
        if (self.LOWER_LEFT.LON > self.UPPER_RIGHT.LON) or (
            self.LOWER_LEFT.LAT > self.UPPER_RIGHT.LAT
        ):
            print(
                f"THE CALL WITH THE ERROR IS: {self.DATE}, {self.LOWER_LEFT.LON}, {self.LOWER_LEFT.LAT}, {self.UPPER_RIGHT.LON}, {self.UPPER_RIGHT.LAT}, {self.UNITS}"
            )
            raise InvalidBoundingBox(self.LOWER_LEFT, self.UPPER_RIGHT)

    def change_units(self, new_units: str, inplace: bool = False):
        if inplace:
            self.LOWER_LEFT.change_units(new_units, inplace=True)
            self.UPPER_RIGHT.change_units(new_units, inplace=True)
            self._update_centre_point()
            self.UNITS = new_units
            return self

        new_lower_left = self.LOWER_LEFT.change_units(new_units)
        new_upper_right = self.UPPER_RIGHT.change_units(new_units)

        return BoundingBox(
            date=self.DATE,
            lon_min=new_lower_left.LON,
            lat_min=new_lower_left.LAT,
            lon_max=new_upper_right.LON,
            lat_max=new_upper_right.LAT,
            units=new_units,
        )

    def get_centre_point(self, as_point: bool = False):
        if as_point:
            return Point(
                date=self.DATE,
                lon=(self.LOWER_LEFT.LON + self.UPPER_RIGHT.LON) / 2,
                lat=(self.LOWER_LEFT.LAT + self.UPPER_RIGHT.LAT) / 2,
                units=self.UNITS,
            )

        return (self.LOWER_LEFT + self.UPPER_RIGHT) / 2

    def get_position_angle(self):
        return self.CENTRE_POINT.get_position_angle()

    def get_distance_to_sun_centre(self):
        return self.CENTRE_POINT.get_distance_to_sun_centre()

    def get_cartesian_centre_point(self):
        return self.CENTRE_POINT.get_cartesian_coords()

    def get_raw_bbox(self):
        return [self.LOWER_LEFT.get_raw_coords(), self.UPPER_RIGHT.get_raw_coords()]

    def get_cartesian_bbox(self):
        return [
            self.LOWER_LEFT.get_cartesian_coords(),
            self.UPPER_RIGHT.get_cartesian_coords(),
        ]

    def get_skycoord_bbox(self):
        return SkyCoord(self.get_raw_bbox(), unit=self.UNITS, frame=self.FRAME)

    def _update_centre_point(self):
        self.CENTRE_POINT = self.get_centre_point()

    def rotate_bbox(self, date, keep_shape=False, inplace: bool = False):
        new_date = Time(date, format="iso")
        new_frame = HeliographicStonyhurst(obstime=new_date)

        if keep_shape:
            new_centre = self.get_centre_point(as_point=True).rotate_coords(new_date)

            width = self.UPPER_RIGHT.LON - self.LOWER_LEFT.LON
            height = self.UPPER_RIGHT.LAT - self.LOWER_LEFT.LAT

            new_lower_left = Point(
                date=new_date,
                lon=new_centre.LON - width / 2,
                lat=new_centre.LAT - height / 2,
                units=self.UNITS,
            )

            new_upper_right = Point(
                date=new_date,
                lon=new_centre.LON + width / 2,
                lat=new_centre.LAT + height / 2,
                units=self.UNITS,
            )
        else:
            new_lower_left = self.LOWER_LEFT.rotate_coords(new_date)
            new_upper_right = self.UPPER_RIGHT.rotate_coords(new_date)

        if inplace:
            self.DATE = new_date
            self.LOWER_LEFT = new_lower_left
            self.UPPER_RIGHT = new_upper_right
            self.FRAME = new_frame
            self._update_centre_point()
            return self

        return BoundingBox(
            date=new_date,
            lon_min=new_lower_left.LON,
            lat_min=new_lower_left.LAT,
            lon_max=new_upper_right.LON,
            lat_max=new_upper_right.LAT,
            units=self.UNITS,
        )

    def is_point_inside(self, point: Point):
        # Check difference in dates is larger than 1 hours
        if abs((self.DATE - point.DATE).to_value("hour")) > 1:
            bbox_coords = self.rotate_bbox(point.DATE).get_raw_bbox()
        else:
            bbox_coords = self.get_raw_bbox()

        if (bbox_coords[0][0] <= point.LON <= bbox_coords[1][0]) and (
            bbox_coords[0][1] <= point.LAT <= bbox_coords[1][1]
        ):
            return True
        else:
            return False

    def get_projected_point_distance(self, point: Point):
        if self.is_point_inside(point):
            return 0

        # I'm not sure if I ever use this by giving it a non-rotated point
        # I think I don't but just in case I kept the else
        if type(self) == RotatedBoundingBox:
            rotated_bbox = self
        else:
            rotated_bbox = self.rotate_bbox(point.DATE)

        bbox_coords = [
            rotated_bbox.LOWER_LEFT.get_cartesian_coords(),
            rotated_bbox.UPPER_RIGHT.get_cartesian_coords(),
        ]
        point_coords = point.get_cartesian_coords()

        if point_coords[0] < bbox_coords[0][0]:
            if point_coords[1] < bbox_coords[0][1]:
                return np.sqrt(
                    (bbox_coords[0][0] - point_coords[0]) ** 2
                    + (bbox_coords[0][1] - point_coords[1]) ** 2
                )
            elif point_coords[1] > bbox_coords[1][1]:
                return np.sqrt(
                    (bbox_coords[0][0] - point_coords[0]) ** 2
                    + (bbox_coords[1][1] - point_coords[1]) ** 2
                )
            else:
                return bbox_coords[0][0] - point_coords[0]
        elif point_coords[0] > bbox_coords[1][0]:
            if point_coords[1] < bbox_coords[0][1]:
                return np.sqrt(
                    (bbox_coords[1][0] - point_coords[0]) ** 2
                    + (bbox_coords[0][1] - point_coords[1]) ** 2
                )
            elif point_coords[1] > bbox_coords[1][1]:
                return np.sqrt(
                    (bbox_coords[1][0] - point_coords[0]) ** 2
                    + (bbox_coords[1][1] - point_coords[1]) ** 2
                )
            else:
                return point_coords[0] - bbox_coords[1][0]
        elif point_coords[1] > bbox_coords[1][1]:
            return point_coords[1] - bbox_coords[1][1]
        else:
            return bbox_coords[0][1] - point_coords[1]

    def get_angular_point_distance(self, point: Point):
        if self.is_point_inside(point):
            return [0, 0]

        # Check difference in dates is larger than 1 hours
        if abs((self.DATE - point.DATE).to_value("hour")) > 1:
            bbox_coords = self.rotate_bbox(point.DATE).get_raw_bbox()
        else:
            bbox_coords = self.get_raw_bbox()

        point_coords = point.get_raw_coords()

        if point_coords[0] < bbox_coords[0][0]:
            if point_coords[1] < bbox_coords[0][1]:
                return [
                    bbox_coords[0][0] - point_coords[0],
                    bbox_coords[0][1] - point_coords[1],
                ]
            elif point_coords[1] > bbox_coords[1][1]:
                return [
                    bbox_coords[0][0] - point_coords[0],
                    bbox_coords[1][1] - point_coords[1],
                ]
            else:
                return [bbox_coords[0][0] - point_coords[0], 0]
        elif point_coords[0] > bbox_coords[1][0]:
            if point_coords[1] < bbox_coords[0][1]:
                return [
                    (bbox_coords[1][0] - point_coords[0]),
                    (bbox_coords[0][1] - point_coords[1]),
                ]
            elif point_coords[1] > bbox_coords[1][1]:
                return [
                    (bbox_coords[1][0] - point_coords[0]),
                    (bbox_coords[1][1] - point_coords[1]),
                ]
            else:
                return [point_coords[0] - bbox_coords[1][0], 0]
        elif point_coords[1] > bbox_coords[1][1]:
            return [0, point_coords[1] - bbox_coords[1][1]]
        else:
            return [0, bbox_coords[0][1] - point_coords[1]]

    def get_spherical_point_distance(self, point: Point):
        if self.is_point_inside(point):
            return 0

        point_coords = np.array(point.get_raw_coords()) * np.pi / 180
        angular_dist = np.array(self.get_angular_point_distance(point)) * np.pi / 180
        bbox_coords = point_coords + angular_dist
        dist = np.arccos(
            np.sin(point_coords[1]) * np.sin(bbox_coords[1])
            + np.cos(point_coords[1]) * np.cos(bbox_coords[1]) * np.cos(angular_dist[0])
        )

        return dist


class RotatedBoundingBox(BoundingBox):
    def __init__(
        self,
        date,
        lon_min: float,
        lat_min: float,
        lon_max: float,
        lat_max: float,
        units: str = "deg",
    ):
        super().__init__(
            date=date,
            lon_min=lon_min,
            lat_min=lat_min,
            lon_max=lon_max,
            lat_max=lat_max,
            units=units,
        )

    def rotate_bbox(self, date, inplace: bool = False):
        raise TypeError("Can't rotate a RotatedBoundingBox")
