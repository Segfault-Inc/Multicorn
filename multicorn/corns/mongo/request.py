class MongoRequest():

    def __init__(self):
        self.spec = {}
        self.count = False
        self.one = False
        #    fields = {}

    def __repr__(self):
        return "MongoRequest(spec=%r)" % self.spec
