import subprocess
import sys
from setuptools import setup, find_packages, Extension


import os
from sys import platform
from setuptools.command.install import install
from distutils.command.build import build

# hum... borrowed from psycopg2
def get_pg_config(kind, pg_config="pg_config"):
    p = subprocess.Popen([pg_config, '--%s' % kind], stdout=subprocess.PIPE)
    r = p.communicate()
    r = r[0].strip().decode('utf8')
    if not r:
        raise Warning(p[2].readline())
    return r

include_dirs = [get_pg_config(d) for d in ("includedir", "pkgincludedir", "includedir-server")]

multicorn_utils_module = Extension('multicorn._utils',
        include_dirs=include_dirs,
        extra_compile_args = ['-shared'],
        sources=['src/utils.c'])

requires=[]

if sys.version_info[0] == 2:
    if sys.version_info[1] == 6:
        requires.append("ordereddict")
    elif sys.version_info[1] < 6:
        sys.exit("Sorry, you need at least python 2.6 for Multicorn")

class MulticornBuild(build):
  def run(self):
    # Original build
    build.run(self)
    r = subprocess.check_output(['/usr/bin/make', 'multicorn.so'])
    r = r.strip().decode('utf8')
    if not r:
      raise Warning(p[2].readline())
    # After original build

execfile('multicorn.control')


setup(
    name='multicorn',
    # version='__VERSION__',
    version=default_version,
    author='Kozea',
    license='Postgresql',
    description='Multicorn Python bindings for Postgres 9.5+ FDW',
    long_description='Multicorn is a PostgreSQL 9.5+ extension meant to make Foreign Data Wrapper development easy, by allowing the programmer to use the Python programming language.',

    options={'bdist_rpm': {'post_install': 'rpm/post_install.sh',
                           'pre_uninstall': 'rpm/pre_uninstall.sh',
                           'requires': 'postgresql95-server',
                           }},
    package_dir={'': 'python'},
    packages=['multicorn', 'multicorn.fsfdw'],
    ext_modules=[multicorn_utils_module],
    data_files=[
        ('%s/extension' % get_pg_config('sharedir'), ['multicorn.control', 'sql/multicorn.sql', 'doc/multicorn.md']),
        (get_pg_config('libdir'), ['multicorn.so'])
        ],
    cmdclass={
        'build': MulticornBuild,
        }
    )
