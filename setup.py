#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Multicorn Setup File
====================

Multicorn packager and installer.

"""

from setuptools import setup, find_packages


VERSION = "git"


setup(
    name="Multicorn",
    version=VERSION,
    description="Content Management Library",
    long_description=__doc__,
    author="Kozea",
    author_email="guillaume.ayoub@kozea.fr",
    url="http://www.multicorn.org/",
    download_url=\
        "http://multicorn.org/src/multicorn/Multicorn-%s.tar.gz" % VERSION,
    license="BSD",
    platforms="Any",
    packages=find_packages(exclude=["*._test", "*._test.*", "test.*", "test"]),
    provides=["multicorn"],
    package_data={"multicorn": ["access_point/xml/xml2rst.xsl"]},
    extras_require={
        "docutils": ["docutils>=0.6"],
        "lxml": ["lxml>=2.0"],
        "SQLAlchemy": ["sqlalchemy>=0.6"]},
    keywords=["web", "framework", "database"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Environment :: Web Environment",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"])
