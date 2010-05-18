from kalamar import site
from kalamar.config import baseparser


class Site(site.Site):
    
    def __init__(self,filename,**kwargs):
        super(Site,self).__init__(baseparser.parse(filename),**kwargs)


