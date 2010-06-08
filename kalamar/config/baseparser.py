import ConfigParser
import os

from . import Config


def parse(config_filename):
    configs = []    
    config_parser = ConfigParser.RawConfigParser()
    if not config_parser.read(config_filename):
        raise self.FileNotFoundError(config_filename)
    basedir = os.path.dirname(config_filename)
    for section in config_parser.sections():
        items = dict(config_parser.items(section))
        url = items.pop("url")
        parser = items.pop("parser") if "parser" in items else None
        properties = {}
        for prop in items.pop("properties").split("/"):
            key, value = prop.split("=")[0:2]
            properties[key] = value
        configs.append(Config(url, section, properties, items, parser, basedir))
    return configs
