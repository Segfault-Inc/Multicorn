# -*- coding: utf-8 -*-
import os.path
from nose.tools import eq_
from kraken.site import Site


def test_all_engines():
    folder = os.path.dirname(__file__)
    site = Site(folder, os.path.join(folder, 'templates'))
    def test_one_engine(engine):
        if engine != 'str-format':
            # str-format does not support default values
            r = site.render_template(engine, 'hello.%s.html' % engine).strip()
            #assert isinstance(r, unicode)
            eq_(r, u'<!DOCTYPE html>\n<html><body>Hello × World!</body></html>')
        r = site.render_template(engine, 'hello.%s.html' % engine, {'name': 'Python'}).strip()
        #assert isinstance(r, unicode)
        eq_(r, u'<!DOCTYPE html>\n<html><body>Hello × Python!</body></html>')
         
    for engine in site.engines:
        yield test_one_engine, engine
