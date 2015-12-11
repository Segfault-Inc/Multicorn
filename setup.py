import subprocess
import sys
from setuptools import setup, find_packages, Extension

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

setup(
 name='multicorn',
 version='__VERSION__',
 author='Kozea',
 license='Postgresql',
 package_dir={'': 'python'},
 packages=['multicorn', 'multicorn.fsfdw'],
 ext_modules = [multicorn_utils_module]
)
