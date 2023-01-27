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
    def __init__(self, date, PA, width, linear_speed = None, halo: bool = False, seen_only_in: int = 0):

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
        
        if linear_speed is None:
            self.LINEAR_SPEED = None
        else:
            self.LINEAR_SPEED = float(linear_speed) * u.km / u.s

        self.SEEN_ONLY_IN = seen_only_in
        self.LINEAR_TIME_AT_SUN_CENTER = self.calculateApproximateLinearTimeAtSunCentre()

        self.HALO_MAX_DIST_TO_SUN_CENTRE = 0.2 # How far harps can be from Sun centre to be consistent with HALO CME
        self.WIDTH_EXTRA_ANGLE = 10 # Extra angle to sides of CME for Spatial co-ocurrence

    def calculateApproximateLinearTimeAtSunCentre(self) -> Time:
        # The LASCO CME catalogue is not clear
        # They say detection time is first seen in C2
        # But then some are seen only in C3?
        # I'll assume if seen only in C3 then time is at edge of C3
        if self.LINEAR_SPEED is None:
            return None
        elif self.SEEN_ONLY_IN == 2:
            return self.DATE - (3.7 * u.Rsun / self.LINEAR_SPEED).decompose()
        else:
            return self.DATE - (1.5 * u.Rsun / self.LINEAR_SPEED).decompose()
            


    def hasHarpsSpatialCoOcurrence(self, harps: Harps, max_time_diff = 12 * u.min) -> bool:
        # Check times
        if np.abs(harps.T_REC - self.DATE) > max_time_diff:
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
