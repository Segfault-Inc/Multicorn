from bson.code import Code

class MongoRequest():

    def __init__(self):
        self.spec = {}
        self.count = False
        self.one = False
        self.fields = {}

    def __repr__(self):
        return "MongoRequest(spec=%r, fields=%r, count=%r, one=%r)" % (
            self.spec,
            self.fields,
            self.count,
            self.one)
