import json
import os
from kalamar.config import Config

def parse(config_filename):
    """ Parses a kalamar config file in Json format."""
    jsonconfig = json.load(open(config_filename))
    configs = []
    basedir = os.path.dirname(config_filename)
    for config in jsonconfig:
        url = config.pop("url")
        name = config.pop("name")
        parser = config.pop("parser") if "parser" in config else None
        configs.append(Config(url,name,config,parser=parser,basedir=basedir))
    return configs
