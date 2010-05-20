import os


class Config(object):
    """Data class containing the configuration for a calamar access-point.""" 

    def __init__(self, url, name, additional_properties, parser=None, basedir=None,
            default_encoding="utf-8"):
        self.url = url
        self.site = None
        self.name = name
        self.additional_properties = additional_properties 
        self.parser = parser
        self.basedir = basedir
        self.default_encoding = default_encoding 

