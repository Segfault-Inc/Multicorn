from bson.code import Code


class MongoRequest():

    def __init__(self):
        self.where = ""
        self.count = False
        self.one = False
        self.fields = {}

    def __repr__(self):
        return "MongoRequest(where=%r, fields=%r, count=%r, one=%r)" % (
            self.where,
            self.fields,
            self.count,
            self.one)

    def spec(self):
        return {"$where": self.where} if self.where else {}
