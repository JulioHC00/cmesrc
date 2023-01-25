from src.harps.harps import Harps, InvalidBoundingBox
import numpy as np
import pytest

def test_good_initialization_of_harps():
    TIME = "2000-12-23T12:00:00"
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
    TIME = "2000-12-23T12:00:00"
    LON_MIN = 23
    LAT_MIN = 20
    LON_MAX = 20
    LAT_MAX = 28

    with pytest.raises(InvalidBoundingBox):
        Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)


TIME = "2000-12-23T12:00:00"
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

@pytest.mark.depend(on=['test_cartesian_center_coords'])
def test_distance_to_sun_center():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    print(harps.LON_MIN, harps.LON_MAX)
    distance_to_sun_center = np.sqrt(harps.CARTESIAN_CENTER_POINT[0] ** 2 + harps.CARTESIAN_CENTER_POINT[1] ** 2)

    assert np.isclose(distance_to_sun_center, harps.DISTANCE_TO_SUN_CENTRE)
