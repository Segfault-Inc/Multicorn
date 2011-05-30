#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Public Domain

"""
Dyko - A Lightweight Web Framework
==================================

Dyko is a lightweight web framework with original features:

- Support for **multiple storage mechanisms** (databases, filesystems, and
  more) with an **unified data access interface**, giving the possibility to
  use **flexible and evolutive data models**;
- Support for **multiple template engines** (Genshi, Jinja, Mako, and more).

Theses specificities make Dyko particularly suitable for creating:

- Small and simple websites with no database;
- Content management systems with heterogeneous storage engines;
- Web applications with chronic data model evolutions;
- Web applications accessing static data models defined by other applications.

Dyko should run on most of the UNIX-like platforms (Linux, BSD, MacOS X) and
Windows. It is free and open-source software, written in Python, released under
GPL version 3.

For further information, please visit the `Dyko Website
<http://www.dyko.org/>`_.

"""

VERSION = "0.3-git"

from setuptools import setup, find_packages


setup(
    name="Dyko",
    version=VERSION,
    description="A Lightweight Web Framework",
    long_description=__doc__,
    author="Kozea",
    author_email="guillaume.ayoub@kozea.fr",
    url="http://www.dyko.org/",
    download_url="http://www.dyko.org/src/dyko/Dyko-%s.tar.gz" % VERSION,
    license="GNU GPL v3",
    platforms="Any",
    packages=find_packages(
        exclude=["*._test", "*._test.*", "test.*", "test"]),
    provides=["kalamar"],
    package_data={
        "kalamar": ["access_point/xml/xml2rst.xsl"]},
    extras_require={
        "Werkzeug": ["werkzeug>=0.5"],
        "docutils": ["docutils>=0.6"],
        "lxml": ["lxml>=2.0"],
        "SQLAlchemy": ["sqlalchemy>=0.6"]},
    keywords=["web", "framework", "database"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Environment :: Web Environment",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Topic :: Software Development :: Libraries :: Application Frameworks"])
