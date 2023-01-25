import pytest
from src.cmes.cmes import CME, MissmatchInTimes
from src.harps.harps import Harps
from astropy.time import Time
import numpy as np

DATE = "2000-12-23T12:00:00"
PA = 23
WIDTH = 100

def test_cme_initialization():
    cme = CME(DATE, PA, WIDTH)

    test_date = (cme.DATE == Time(DATE))
    test_PA = (cme.PA == PA)
    test_width = (cme.WIDTH == WIDTH)
    test_halo = (cme.HALO == False)

    tests = np.array([test_date, test_PA, test_width, test_halo])

    assert np.all(tests)

HARPS_DATE = "2000-12-23T12:11:00"
MIN_LON = -25
MIN_LAT = 23
MAX_LON =  -20
MAX_LAT = 28

def test_cme_harps_co_ocurrence():
    cme = CME(DATE, PA, WIDTH)
    harps = Harps(HARPS_DATE, MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)


    assert cme.hasHarpsSpatialCoOcurrence(harps)

def test_cme_harps_NO_co_ocurrence():
    MIN_LON = 20
    MIN_LAT = 23
    MAX_LON = 25
    MAX_LAT = 28
    cme = CME(DATE, PA, WIDTH)
    harps = Harps(HARPS_DATE, MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    assert not cme.hasHarpsSpatialCoOcurrence(harps)

def test_HALO_cme_harps_co_ocurrence():
    MIN_LON = 5
    MIN_LAT = 5
    MAX_LON = 7
    MAX_LAT = 7
    cme = CME(DATE, None, WIDTH, halo=True)
    harps = Harps(HARPS_DATE, MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    assert cme.hasHarpsSpatialCoOcurrence(harps)

def test_HALO_cme_harps_NO_co_ocurrence():
    MIN_LON = 80
    MIN_LAT = 80
    MAX_LON = 85
    MAX_LAT = 85
    cme = CME(DATE, None, WIDTH, halo=True)
    harps = Harps(HARPS_DATE, MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    assert not cme.hasHarpsSpatialCoOcurrence(harps)
