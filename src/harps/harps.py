from src.cmesrc.classes import BoundingBox, RotatedBoundingBox

class RotatedHarps(RotatedBoundingBox):
    def __init__(self, date, lon_min, lat_min, lon_max, lat_max, HARPNUM=None):
        super().__init__(
                date=date,
                lon_min=lon_min,
                lat_min=lat_min,
                lon_max=lon_max,
                lat_max=lat_max
                )
        self.HARPNUM = HARPNUM

class Harps(BoundingBox):
    def __init__(self, date, lon_min, lat_min, lon_max, lat_max, HARPNUM=None):
        super().__init__(
                date=date,
                lon_min=lon_min,
                lat_min=lat_min,
                lon_max=lon_max,
                lat_max=lat_max
                )
        self.HARPNUM = HARPNUM

    def rotate_bbox(self, date, inplace:bool = False):
        new_bbox = super().rotate_bbox(date=date, inplace=inplace)

        return RotatedHarps(
                date = new_bbox.DATE,
                lon_min = new_bbox.LOWER_LEFT.LON,
                lat_min = new_bbox.LOWER_LEFT.LAT,
                lon_max = new_bbox.UPPER_RIGHT.LON,
                lat_max = new_bbox.UPPER_RIGHT.LAT,
                HARPNUM = self.HARPNUM
                )
