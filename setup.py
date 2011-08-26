#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Multicorn
=========

Multicorn is a data access library.

"""

import sys
from setuptools import setup, find_packages

# Use a time-based version number with ridiculous precision as pip in tox
# does not reinstall the same version.
import datetime
VERSION = "git-" + datetime.datetime.now().isoformat()


options = dict(
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
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.1",  # TODO: test it?
        "Programming Language :: Python :: 3.2",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"])


if sys.version_info >= (3,):
    options['use_2to3'] = True
    # pymongo is not (yet) ported to Python 3.
    options['install_requires'].remove('pymongo')


setup(**options)
