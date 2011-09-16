from importlib import import_module


class Qual(object):

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def __repr__(self):
        return "%s %s %s" % (self.field_name, self.operator, self.value)


class ForeignDataWrapper(object):

    def __init__(self, fdw_options, fdw_columns):
        print "Initializing fdw"

    def execute(self, quals):
        pass


def getClass(module_path):
    module_path.split(".")
    wrapper_class = module_path.split(".")[-1]
    module_name = ".".join(module_path.split(".")[:-1])
    print "Importing module %s" % module_name
    module = import_module(module_name)
    print "Getting wrapper_class %s" % wrapper_class
    return getattr(module, wrapper_class)
