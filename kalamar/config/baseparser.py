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
        for attr in ["parser_aliases" , "storage_aliases"]:
            if attr in items:
                items[attr] = [name.split("=",1) for name in items[attr].split("/") if name]
        configs.append(Config(url,section,items,parser,basedir))
    return configs
