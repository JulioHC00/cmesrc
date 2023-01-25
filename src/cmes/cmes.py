from src.harps.harps import Harps
from astropy.time import Time
from astropy import units as u
import numpy as np

class MissmatchInTimes(Exception):
    """
    Raised when Harps and CME recording time are different by more than 12 min"
    """
    def __init__(self, HARPS_TIME: Time, CME_TIME: Time):
        self.HARPS_TIME = HARPS_TIME
        self.CME_TIME = CME_TIME
        self.TIME_DIFFERENCE = np.abs(self.HARPS_TIME - self.CME_TIME).to_value(u.min)

        self.message = f"HARPS position with date {self.HARPS_TIME} is more than 12m. ({self.TIME_DIFFERENCE:.2f} min.) away from the CME observation time {self.CME_TIME}"
        super().__init__(self.message)


class CME:
    def __init__(self, date, PA, width, halo: bool = False):

        if width > 360:
            raise ValueError(f"Width {width} must be <= 360 deg.")

        if halo:
            self.PA = None
        else:
            if not 0 <= PA <= 360:
                raise ValueError(f"Principal angle {PA} must be within 0 and 360")

            self.PA = float(PA)

        self.DATE = Time(date)
        self.WIDTH = float(width)
        self.HALO = halo
        self.HALO_MAX_DIST_TO_SUN_CENTRE = 0.2 # How far harps can be from Sun centre to be consistent with HALO CME
        self.WIDTH_EXTRA_ANGLE = 10 # Extra angle to sides of CME for Spatial co-ocurrence

    def hasHarpsSpatialCoOcurrence(self, harps: Harps) -> bool:
        # Check times
        if np.abs(harps.T_REC - self.DATE) > 12 * u.min:
            raise MissmatchInTimes(harps.T_REC, self.DATE)

        if self.HALO:
            if harps.DISTANCE_TO_SUN_CENTRE < self.HALO_MAX_DIST_TO_SUN_CENTRE:
                return True
            return False

        harps_angle_dist_to_PA = np.abs(harps.POSITION_ANGLE - self.PA)

        # If the angle distance is larger than 180, then take the other side (360 - >180) is the smaller angle.
        if harps_angle_dist_to_PA > 180:
            harps_angle_dist_to_PA = 360 - harps_angle_dist_to_PA

        # Must be within the width plus 10 deg.
        if harps_angle_dist_to_PA < (self.WIDTH / 2 + 10):
            return True
        return False
