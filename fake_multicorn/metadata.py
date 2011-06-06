
class Metadata(object):
    def __init__(self):
        self.access_points = {}

    def register(self, access_point):
        access_point.bind(self) # Do this first as it may raise.
        self.access_points[access_point.name] = access_point
        return access_point
