from src.harps.harps import Harps, RotatedHarps, InvalidBoundingBox
from sunpy.coordinates import HeliographicStonyhurst, propagate_with_solar_surface
import numpy as np
import pytest

def test_good_initialization_of_harps():
    TIME = "2000-12-23 12:00:00"
    LON_MIN = 23
    LAT_MIN = 24
    LON_MAX = 26
    LAT_MAX = 28
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    test_time = (harps.T_REC == TIME)
    test_coords = np.array([
        (harps.LON_MIN == LON_MIN),
        (harps.LAT_MIN == LAT_MIN),
        (harps.LON_MAX == LON_MAX),
        (harps.LAT_MAX == LAT_MAX),
        ])

    test = test_time & np.all(test_coords)

    assert test

def test_invalid_bounding_box():
    TIME = "2000-12-23 12:00:00"
    LON_MIN = 23
    LAT_MIN = 20
    LON_MAX = 20
    LAT_MAX = 28

    with pytest.raises(InvalidBoundingBox):
        Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)


TIME = "2000-12-23 12:00:00"
LON_MIN = 10
LAT_MIN = 20
LON_MAX = 30
LAT_MAX = 40

def test_center_point_coords():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    assert np.all(harps.RAW_CENTER_POINT == np.array([20, 30]))

@pytest.mark.depends(on=['test_center_point_coords'])
def test_cartesian_center_coords():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    cartesian_centre = np.array([
        np.cos(harps.RAW_CENTER_POINT[1] * np.pi / 180) * np.sin(harps.RAW_CENTER_POINT[0] * np.pi / 180),
        np.sin(harps.RAW_CENTER_POINT[1] * np.pi / 180)
        ])

    assert np.all(np.isclose(harps.CARTESIAN_CENTER_POINT, cartesian_centre))

@pytest.mark.depends(on=['test_cartesian_center_coords'])
def test_position_angle():
    LON_MIN = -40
    LAT_MIN = 20
    LON_MAX = -20
    LAT_MAX = 40
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    position_angle = np.arctan2(harps.CARTESIAN_CENTER_POINT[1], harps.CARTESIAN_CENTER_POINT[0]) * 180 / np.pi - 90
    assert (np.isclose(position_angle, harps.POSITION_ANGLE)) and (position_angle < 90)

@pytest.mark.depends(on=['test_cartesian_center_coords'])
def test_distance_to_sun_center():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    distance_to_sun_center = np.sqrt(harps.CARTESIAN_CENTER_POINT[0] ** 2 + harps.CARTESIAN_CENTER_POINT[1] ** 2)

    assert np.isclose(distance_to_sun_center, harps.DISTANCE_TO_SUN_CENTRE)

def test_non_rotated():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    assert harps.is_rotated() == False

def test_rotated():
    harps = RotatedHarps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    assert harps.is_rotated() == True

def test_rotation_returns_RotatedHarps():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    rotated_harps = harps.rotate_coords("2000-12-23 12:00:01")

    assert type(rotated_harps) == RotatedHarps

def test_rotated_coordinates_values():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    NEW_TIME = "2000-12-23 16:00:00"

    rotated_harps = harps.rotate_coords(NEW_TIME)

    original_coords = harps.get_skycoord_bounding_box()

    new_frame = HeliographicStonyhurst(obstime=NEW_TIME)

    with propagate_with_solar_surface():
        new_coordinates = original_coords.transform_to(new_frame)

    separation = new_coordinates.separation(rotated_harps.get_skycoord_bounding_box()).degree

    assert np.all(separation < 1e-4)
