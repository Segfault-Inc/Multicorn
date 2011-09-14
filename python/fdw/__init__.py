from importlib import import_module


class ForeignDataWrapper(object):

    def __init__(self, fdw_options):
        print "Initializing fdw"

    def execute(self, options):
        pass


def getClass(module_path):
    module_path.split(".")
    wrapper_class = module_path.split(".")[-1]
    module_name = ".".join(module_path.split(".")[:-1])
    print "Importing module %s" % module_name
    module = import_module(module_name)
    print "Getting wrapper_class %s" % wrapper_class
    return getattr(module, wrapper_class)
