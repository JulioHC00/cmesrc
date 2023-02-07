from src.cmesrc.classes import Point
import numpy as np
from astropy.time import Time
from src.harps.harps import Harps

class Dimming():

    def __init__(self, date, lon, lat):
        self.point = Point(date, lon, lat)

class OffDiskDimming():

    def __init__(self, date, x, y):

        self.DATE = Time(date)
        self.X = x
        self.Y = y
        self.R = np.sqrt(x**2 + y**2)
        self.PA = self.getPA()
        

    def getPA(self):
        position_angle = np.arctan2(self.Y, self.X) * 180 / np.pi

        if 0 <= position_angle <= 90:
            position_angle += 270
        elif 90 < position_angle <= 180:
            position_angle -= 90
        elif position_angle < 0:
            position_angle += 270

        return position_angle

    def get_ang_dist_harps(self, harps:Harps):
        rotated_harps = harps.rotate_bbox(self.DATE)

        return np.abs(self.PA - harps.get_position_angle())
