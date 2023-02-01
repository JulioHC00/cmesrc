from src.harps.harps import Harps, RotatedHarps
from src.cmesrc.exception_classes import InvalidBoundingBox
from sunpy.coordinates import HeliographicStonyhurst, propagate_with_solar_surface
from src.cmesrc.classes import BoundingBox, RotatedBoundingBox
import numpy as np
import pytest

def test_good_initialization_of_harps():
    TIME = "2000-12-23 12:00:00"
    LON_MIN = 23
    LAT_MIN = 24
    LON_MAX = 26
    LAT_MAX = 28
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    assert np.all([
        np.all([[23,24], [26,28]] == harps.get_raw_bbox())
        ])

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

    assert np.all(harps.CENTRE_POINT.get_raw_coords() == np.array([20, 30]))

@pytest.mark.depends(on=['test_center_point_coords'])
def test_cartesian_center_coords():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    centre_point = harps.CENTRE_POINT
    cartesian_centre = np.array([
        np.cos(centre_point.LAT * np.pi / 180) * np.sin(centre_point.LON * np.pi / 180),
        np.sin(centre_point.LAT * np.pi / 180)
        ])

    assert np.all(np.isclose(harps.get_cartesian_centre_point(), cartesian_centre))

@pytest.mark.depends(on=['test_cartesian_center_coords'])
def test_position_angle():
    LON_MIN = -40
    LAT_MIN = 20
    LON_MAX = -20
    LAT_MAX = 40
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    centre_point = harps.get_cartesian_centre_point()

    position_angle = np.arctan2(centre_point[1], centre_point[0]) * 180 / np.pi - 90
    assert (np.isclose(position_angle, harps.get_position_angle())) and (position_angle < 90)

@pytest.mark.depends(on=['test_cartesian_center_coords'])
def test_distance_to_sun_center():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    centre_point = harps.get_cartesian_centre_point()
    distance_to_sun_center = np.sqrt(centre_point[0] ** 2 + centre_point[1] ** 2)

    assert np.isclose(distance_to_sun_center, harps.get_distance_to_sun_centre())

def test_non_rotated():
    assert issubclass(Harps, BoundingBox)

def test_rotated():
    harps = RotatedHarps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)
    assert issubclass(RotatedHarps, RotatedBoundingBox)

def test_rotation_returns_RotatedHarps():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    rotated_harps = harps.rotate_bbox("2000-12-23 12:00:01")

    assert type(rotated_harps) is RotatedHarps

def test_rotated_coordinates_values():
    harps = Harps(TIME, LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    NEW_TIME = "2000-12-23 16:00:00"

    rotated_harps = harps.rotate_bbox(NEW_TIME)

    original_coords = harps.get_skycoord_bbox()

    new_frame = HeliographicStonyhurst(obstime=NEW_TIME)

    with propagate_with_solar_surface():
        new_coordinates = original_coords.transform_to(new_frame)

    separation = new_coordinates.separation(rotated_harps.get_skycoord_bbox()).degree

    assert np.all(separation < 1e-4)
