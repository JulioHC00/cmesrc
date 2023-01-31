class InvalidBoundingBox(Exception):
    """
    To be raised when a bounding box is not valid
    """
    def __init__(self, LOWER_LEFT, UPPER_RIGHT):
        # Set values
        self.LOWER_LEFT = LOWER_LEFT
        self.UPPER_RIGHT = UPPER_RIGHT
        self.message = f"Bounding box [{self.LOWER_LEFT.get_raw_coords()}, {self.UPPER_RIGHT.get_raw_coords()}] is not valid"
        super().__init__(self.message)
