import subprocess
from setuptools import setup, find_packages, Extension


import os
from sys import platform
#from setuptools import setup
from setuptools.command.install import install
from distutils.command.build import build


# hum... borrowed from psycopg2
def get_pg_config(kind, pg_config="pg_config"):
    r = subprocess.check_output([pg_config, '--%s' % kind])
    r = r.strip().decode('utf8')
    if not r:
        raise Warning(p[2].readline())
    return r

include_dirs = [get_pg_config(d) for d in ("includedir", "pkgincludedir", "includedir-server")]

multicorn_utils_module = Extension('multicorn._utils',
        include_dirs=include_dirs,
        extra_compile_args = ['-shared'],
        sources=['src/utils.c'])


class MulticornBuild(build):
  def run(self):
    # Original build
    build.run(self)
    r = subprocess.check_output(['/usr/bin/make', 'multicorn.so'])
    r = r.strip().decode('utf8')
    if not r:
      raise Warning(p[2].readline())
    # After original build



setup(
  name='multicorn',
  version='__VERSION__',
  author='Kozea',
  license='Postgresql',
  package_dir={'': 'python'},
  packages=['multicorn', 'multicorn.fsfdw'],
  ext_modules = [multicorn_utils_module],
  data_files=[('%s/extension' % get_pg_config('sharedir'), ['multicorn.control', 'sql/multicorn--1.1.1.sql', 'doc/multicorn.md']),
	(get_pg_config('libdir'),['multicorn.so'])
	],
  cmdclass={
    'build':MulticornBuild,
  }
)
