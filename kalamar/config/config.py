class Config(object):
    """Data class containing the configuration for a calamar access-point.""" 

    def __init__(self, url, name, additional_properties, parser=None):
        self.url = url
        self.name = name
        self.additional_properties = additional_properties 
        self.parser = parser

