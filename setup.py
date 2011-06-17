#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Multicorn
=========

Multicorn is a data access library.

"""

from setuptools import setup, find_packages


VERSION = "git"


setup(
    name="Multicorn",
    version=VERSION,
    description="Data access library",
    long_description=__doc__,
    author="Kozea",
    author_email="guillaume.ayoub@kozea.fr",
    url="http://www.multicorn.org/",
#    download_url=\
#        "http://multicorn.org/src/multicorn/Multicorn-%s.tar.gz" % VERSION,
    license="BSD",
    platforms="Any",
    packages=find_packages(),
    install_requires=['SQLAlchemy', 'pymongo'],
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
        "Programming Language :: Python :: 3.1", # TODO: test it?
        "Programming Language :: Python :: 3.2",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"])
