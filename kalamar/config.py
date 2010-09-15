import os
try:
    import json
except ImportError:
    import simplejson as json


class Config(object):
    """Data class containing the configuration for a calamar access-point.""" 
    def __init__(self, url, name, properties, additional_properties,
                 parser=None, basedir=None, default_encoding="utf-8",
                 debug=False, label_attr=None):
        self.url = url
        self.site = None
        self.name = name
        self.properties = properties
        self.additional_properties = additional_properties 
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
        strvalue += "\t additional_props:\n"
        for prop in self.additional_properties:
            strvalue += "\t\t %s = %s\n" % (prop, self.additional_properties[prop])
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
        parser = config.pop("parser") if "parser" in config else None
        properties = config.pop("properties")
        debug = config.pop("debug", False)
        label_attr = config.pop("label_attr", None)
        yield Config(
            url, name, properties, config, parser, basedir, debug, label_attr)

