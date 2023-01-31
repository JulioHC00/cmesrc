import pytest
import numpy as np
import astropy.units as u
from src.cmesrc.classes import Point, BoundingBox
from src.cmesrc.exception_classes import InvalidBoundingBox
from sunpy.coordinates import HeliographicStonyhurst, propagate_with_solar_surface
from astropy.coordinates import SkyCoord
from astropy.time import Time
from copy import deepcopy

DATE = "2020-12-30 23:12:20"
DATE2 = "2020-12-30 24:12:20"
LON = 10
LAT = 40
LON2 = 20
LAT2 = 10
ARCSEC_LON = 3600 * LON
ARCSEC_LAT = 3600 * LAT
DEG_TO_RAD = np.pi / 180

def test_point_initialization():
    point = Point(DATE, LON, LAT)

    assert np.all([
            point.DATE == Time(DATE, format="iso"),
            point.LON == LON,
            point.LAT == LAT,
            point.UNITS == "deg",
            point.FRAME == HeliographicStonyhurst(obstime=DATE)
            ])

def test_point_comparison():
    point1 = Point(DATE, LON, LAT)
    point2 = Point(DATE, LON2, LAT2)
    point3 = Point(DATE, LON, LAT)

    assert np.all([
        point1 == point1,
        not point1 == point2,
        point1 == point3
        ])

def test_point_addition():
    point1 = Point(DATE, LON, LAT)
    point2 = Point(DATE, LON2, LAT2)

    sum_point = point1 + point2

    assert np.all([
            sum_point.DATE == Time(DATE, format="iso"),
            sum_point.LON == LON + LON2,
            sum_point.LAT == LAT + LAT2,
            sum_point.UNITS == "deg",
            sum_point.FRAME == HeliographicStonyhurst(obstime=DATE)
            ])

def test_point_addition_exception():
    point1 = Point(DATE, LON, LAT)

    with pytest.raises(TypeError):
        sum_point = point1 + 2

def test_point_number_division():
    point = Point(DATE, LON, LAT)
    divided_point = point / 2

    assert np.all([
            divided_point.DATE == Time(DATE, format="iso"),
            divided_point.LON == LON / 2,
            divided_point.LAT == LAT / 2,
            divided_point.UNITS == "deg",
            divided_point.FRAME == HeliographicStonyhurst(obstime=DATE)
            ])

def test_change_point_units_NOT_inplace():
    point = Point(DATE, LON, LAT)
    changed_point = point.change_units("arcsec")

    assert np.all([
        point.LON == LON,
        point.LAT == LAT,
        point.UNITS == "deg",
        changed_point.LON == ARCSEC_LON,
        changed_point.LAT == ARCSEC_LAT,
        changed_point.UNITS == "arcsec"
        ])

def test_change_point_units_inplace():
    point = Point(DATE, LON, LAT)
    point.change_units("arcsec", inplace=True)

    assert np.all([
        point.LON == ARCSEC_LON,
        point.LAT == ARCSEC_LAT,
        point.UNITS == "arcsec"
        ])

def test_get_skycoord():
    point = Point(DATE, LON, LAT)

    true_skycoord = SkyCoord(LON, LAT, unit="deg", frame=HeliographicStonyhurst(obstime=Time(DATE, format="iso")))

    assert true_skycoord == point.get_skycoord()

def test_raw_coords():
    point = Point(DATE, LON, LAT)
    true_coords = [LON, LAT]

    assert true_coords == point.get_raw_coords()

def test_get_cartesian_coords():
    point = Point(DATE, LON, LAT)

    true_cartesian_coords = [
            np.cos(LAT * DEG_TO_RAD) * np.sin(LON * DEG_TO_RAD),
            np.sin(LAT * DEG_TO_RAD)
            ]

    assert np.all(np.isclose(true_cartesian_coords, point.get_cartesian_coords()))

@pytest.mark.depends(on=['test_get_cartesian_coords'])
def test_get_position_angle():
    LON = -30
    LAT = 30
    point = Point(DATE, LON, LAT)

    true_cartesian_coords = point.get_cartesian_coords()

    true_position_angle = np.arctan2(true_cartesian_coords[1], true_cartesian_coords[0]) * 180 / np.pi - 90

    assert np.isclose(true_position_angle, point.get_position_angle()) and (true_position_angle < 90)

@pytest.mark.depends(on=['test_get_cartesian_coords'])
def test_get_distance_to_sun_centre():
    point = Point(DATE, LON, LAT)
    true_cartesian_coords = point.get_cartesian_coords()

    true_distance_to_sun_centre = np.sqrt(true_cartesian_coords[0] ** 2 + true_cartesian_coords[1] ** 2)

    assert true_distance_to_sun_centre == point.get_distance_to_sun_centre()


def test_rotate_not_in_place():
    new_date = Time(DATE, format="iso") + 2 * u.day

    point = Point(DATE, LON, LAT)
    new_point = point.rotate_coords(new_date)

    point_frame = point.FRAME
    point_coords = point.get_skycoord()

    new_frame = HeliographicStonyhurst(obstime = new_date)

    with propagate_with_solar_surface():
        new_coords = point_coords.transform_to(new_frame)

    new_lon, new_lat = [float(coord) for coord in new_coords.to_string().split()]

    new_skycoord = SkyCoord(new_lon, new_lat, unit="deg", frame=new_frame)

    assert np.all([
        new_point != point,
        new_point.get_skycoord() == new_skycoord
        ])

def test_rotate_in_place():
    new_date = Time(DATE, format="iso") + 2 * u.day

    point = Point(DATE, LON, LAT)
    point.rotate_coords(new_date, inplace=True)

    point_frame = point.FRAME
    point_coords = point.get_skycoord()

    new_frame = HeliographicStonyhurst(obstime = new_date)

    with propagate_with_solar_surface():
        new_coords = point_coords.transform_to(new_frame)

    new_lon, new_lat = [float(coord) for coord in new_coords.to_string().split()]

    new_skycoord = SkyCoord(new_lon, new_lat, unit="deg", frame=new_frame)

    assert np.all([
        point.get_skycoord() == new_skycoord
        ])


# TESTING BOUNDING BOXES

DATE = "2020-12-30 23:12:20"
DATE2 = "2020-12-31 00:00:00"
LON_MIN = -30
LAT_MIN = 20
LON_MAX = -10
LAT_MAX = 40

ARCSEC_LON_MIN = LON_MIN * 3600
ARCSEC_LAT_MIN = LAT_MIN * 3600
ARCSEC_LON_MAX = LON_MAX * 3600
ARCSEC_LAT_MAX = LAT_MAX * 3600

DEG_TO_RAD = np.pi / 180

def test_bounding_box_initalization():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    lower_left = Point(
            date = DATE,
            lon = LON_MIN,
            lat = LAT_MIN
            )

    upper_right = Point(
            date = DATE,
            lon = LON_MAX,
            lat = LAT_MAX
            )

    assert np.all([
        bbox.LOWER_LEFT == lower_left,
        bbox.UPPER_RIGHT == upper_right,
        bbox.DATE == DATE,
        bbox.UNITS == "deg",
        bbox.FRAME == HeliographicStonyhurst(obstime=Time(DATE, format="iso"))
        ])

def test_invalid_bounding_box():
    LON_MIN = -30
    LAT_MIN = 20
    LON_MAX = -40
    LAT_MAX = 40

    with pytest.raises(InvalidBoundingBox):
        bbox = BoundingBox(
                date = DATE,
                lon_min = LON_MIN,
                lat_min = LAT_MIN,
                lon_max = LON_MAX,
                lat_max = LAT_MAX
                )

@pytest.mark.depends(on=['test_bounding_box_initalization'])
def test_get_raw_bbox_coords():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    true_bbox_coords = [[LON_MIN, LAT_MIN], [LON_MAX, LAT_MAX]]

    assert np.all(true_bbox_coords == bbox.get_raw_bbox())


@pytest.mark.depends(on=['test_get_raw_bbox_coords'])
def test_change_bbox_units_in_place():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    bbox.change_units("arcsec", inplace=True)

    true_arcsec_bounding_box = [[ARCSEC_LON_MIN, ARCSEC_LAT_MIN], [ARCSEC_LON_MAX, ARCSEC_LAT_MAX]]

    assert np.all([
        np.all(bbox.get_raw_bbox() == true_arcsec_bounding_box),
        bbox.UNITS == "arcsec"
        ])

@pytest.mark.depends(on=['test_get_raw_bbox_coords'])
def test_change_bbox_units_NOT_in_place():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    changed_bbox = bbox.change_units("arcsec")

    true_arcsec_bounding_box = [[ARCSEC_LON_MIN, ARCSEC_LAT_MIN], [ARCSEC_LON_MAX, ARCSEC_LAT_MAX]]

    assert np.all([
        changed_bbox is not bbox,
        np.all(changed_bbox.get_raw_bbox() == true_arcsec_bounding_box),
        changed_bbox.UNITS == "arcsec"
        ])

@pytest.mark.depends(on=['test_point_addition', 'test_point_number_division'])
def test_get_bbox_centre_point():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    true_centre = [-20, 30]

    assert np.all([
        np.all(true_centre == bbox.get_centre_point().get_raw_coords()),
        np.all(true_centre == bbox.CENTRE_POINT.get_raw_coords())
        ])

@pytest.mark.depends(on=['test_get_bbox_centre_point', 'test_get_position_angle'])
def test_bbox_position_angle():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )
    assert bbox.get_position_angle() == bbox.CENTRE_POINT.get_position_angle()

@pytest.mark.depends(on=['test_get_bbox_centre_point', 'test_get_distance_to_sun_centre'])
def test_bbox_distance_to_sun_centre():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )
    assert bbox.get_distance_to_sun_centre() == bbox.CENTRE_POINT.get_distance_to_sun_centre()

@pytest.mark.depends(on=['test_get_bbox_centre_point', 'test_get_cartesian_coords'])
def test_bbox_get_cartesian_centre_point():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )
    assert bbox.get_cartesian_centre_point() == bbox.CENTRE_POINT.get_cartesian_coords()

@pytest.mark.depends(on=['test_raw_coords'])
def test_bbox_get_raw_bbox():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    true_raw_bbox = [bbox.LOWER_LEFT.get_raw_coords(),
                     bbox.UPPER_RIGHT.get_raw_coords()]

    assert np.all(bbox.get_raw_bbox() == true_raw_bbox)

@pytest.mark.depends(on=['test_get_skycoord'])
def test_bbox_get_skycoord():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    true_skycoord = SkyCoord([bbox.LOWER_LEFT.get_raw_coords(), bbox.UPPER_RIGHT.get_raw_coords()], unit=bbox.UNITS, frame=HeliographicStonyhurst(obstime=bbox.DATE))

    assert np.all(true_skycoord == bbox.get_skycoord_bbox())

@pytest.mark.depends(on=['test_rotate_in_place', 'test_rotate_not_in_place'])
def test_rotate_bbox_inplace():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    original_lower_left = bbox.LOWER_LEFT
    original_upper_right = bbox.UPPER_RIGHT

    rotated_lower_left = original_lower_left.rotate_coords(DATE2)
    rotated_upper_right = original_upper_right.rotate_coords(DATE2)

    rotated_centre = (rotated_lower_left + rotated_upper_right) / 2

    bbox.rotate_bbox(DATE2, inplace=True)

    assert np.all([
        bbox.LOWER_LEFT == rotated_lower_left,
        bbox.UPPER_RIGHT == rotated_upper_right,
        bbox.DATE == DATE2,
        bbox.FRAME == HeliographicStonyhurst(obstime=Time(DATE2)),
        bbox.CENTRE_POINT == rotated_centre
        ])

@pytest.mark.depends(on=['test_rotate_in_place', 'test_rotate_not_in_place'])
def test_rotate_bbox_inplace():
    bbox = BoundingBox(
            date = DATE,
            lon_min = LON_MIN,
            lat_min = LAT_MIN,
            lon_max = LON_MAX,
            lat_max = LAT_MAX
            )

    original_lower_left = bbox.LOWER_LEFT
    original_upper_right = bbox.UPPER_RIGHT

    rotated_lower_left = original_lower_left.rotate_coords(DATE2)
    rotated_upper_right = original_upper_right.rotate_coords(DATE2)

    rotated_centre = (rotated_lower_left + rotated_upper_right) / 2
    original_centre = (original_lower_left + original_upper_right) / 2

    rotated_bbox = bbox.rotate_bbox(DATE2)

    assert np.all([
        rotated_bbox.LOWER_LEFT == rotated_lower_left,
        rotated_bbox.UPPER_RIGHT == rotated_upper_right,
        rotated_bbox.DATE == DATE2,
        rotated_bbox.FRAME == HeliographicStonyhurst(obstime=Time(DATE2)),
        rotated_bbox.CENTRE_POINT == rotated_centre,
        bbox.LOWER_LEFT == original_lower_left,
        bbox.UPPER_RIGHT == original_upper_right,
        bbox.DATE == DATE,
        bbox.FRAME == HeliographicStonyhurst(obstime=Time(DATE)),
        bbox.CENTRE_POINT == original_centre
        ])

