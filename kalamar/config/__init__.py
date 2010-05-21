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

    def __str__(self):
        strvalue = "Config " + self.name + " : \n" 
        strvalue += "\t url: " + self.url + "\n"
        strvalue += "\t parser: " + str(self.parser or None) +"\n"
        strvalue += "\t basedir: " + self.basedir +"\n"
        strvalue += "\t default_encoding: " + self.default_encoding +"\n"
        strvalue += "\t additional_props: \n"
        for prop in self.additional_properties:
            strvalue += "\t " + prop + " = " + str(self.additional_properties[prop]) + "\n"
        return strvalue


        


