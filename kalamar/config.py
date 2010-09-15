import os
import decimal
import io
try:
    import json
except ImportError:
    import simplejson as json


PROPERTY_TYPES = set(str, int, float, decimal.Decimal, io.IOBase)

class Property(object):
    def __init__(self, name, property_type, identity=False, auto=False,
                 default=None, mandatory=False, relation=None,
                 remote_property=None):
        self.name = name
        self.prop_type = prop_type
        self.identity = identity
        self.auto = auto
        self.default = default
        self.mandatory = mandatory
        self.relation = relation
        self.remote_property = remote_property


class Config(object):
    """Data class containing the configuration for a calamar access-point.""" 
    def __init__(self, url, name, properties, additional_properties,
                 default_encoding="utf-8", debug=False, label_attr=None):
        self.url = url
        self.site = None
        self.name = name
        self.properties = properties
        self.parser = parser
        self.basedir = basedir
        self.default_encoding = default_encoding
        self.debug = debug
        self.label_attr = label_attr

    def __str__(self):
        strvalue = "Config %s:\n" % self.name
        strvalue += "\t url: %s\n" % self.url
        strvalue += "\t basedir: %s\n" % self.basedir
        strvalue += "\t parser: %s\n" % self.parser
        strvalue += "\t default_encoding: %s\n" % self.default_encoding
        strvalue += "\t managed_properties:\n"
        for prop in self.properties:
            strvalue += "\t %s = %s\n" % (prop, self.properties[prop])
        return strvalue

def parse(config_filename):
    """Parse a kalamar config file in Json format."""
    jsonconfig = json.load(open(config_filename))
    basedir = os.path.dirname(config_filename)
    for config in jsonconfig:
        url = config.pop("url")
        name = config.pop("name")
        properties = dict(
            (prop_name,Property(prop_name, **prop_values)) 
            for prop_name,prop_values in config.pop("properties").items())
        label = config.pop("label", None)
        yield Config(
            url, name, properties, config, basedir, debug, label)

