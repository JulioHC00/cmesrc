from src.cmesrc.classes import Point
class Flare():
    def __init__(self, date, lon, lat, xr_class=None):
        self.point = Point(date, lon, lat)
        self.XR_CLASS = xr_class
