import os
from kalamar.config import Config
try:
        import json
except ImportError:
        import simplejson as json

def parse(config_filename):
    """ Parses a kalamar config file in Json format."""
    jsonconfig = json.load(open(config_filename))
    configs = []
    basedir = os.path.dirname(config_filename)
    for config in jsonconfig:
        url = config.pop("url")
        name = config.pop("name")
        parser = config.pop("parser") if "parser" in config else None
        properties = config.pop("properties")
        configs.append(Config(url, name, properties, config, parser=parser, basedir=basedir))
    return configs
