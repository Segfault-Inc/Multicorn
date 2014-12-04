import subprocess
from setuptools import setup, find_packages, Extension

setup(
 name='multicorn_doc',
 version='__VERSION__',
 author='Kozea',
 license='Postgresql',
 requires=['sphinx', 'sphinx-autobuild']
)
