class Type(object):

    def __init__(self, corn=None, name=None, type=object):
        self.corn = corn
        self.name = name
        self.type = type

class Dict(Type):

    def __init__(self, corn=None, name=None, mapping=None):
        super(Dict, self).__init__(corn=corn, name=name, type=dict)
        self.mapping = mapping

class List(Type):

    def __init__(self, corn=None, name=None, inner_type=Type(type=object)):
        super(Dict, self).__init__(corn=corn, name=name, type=list)
        self.inner_type = inner_type

