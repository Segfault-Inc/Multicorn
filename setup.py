import popen2
from setuptools import setup, find_packages, Extension

# hum... borrowed from psycopg2
def get_pg_config(kind, pg_config="pg_config"):
    p = popen2.popen3(pg_config + " --" + kind)
    r = p[0].readline().strip()
    if not r:
        raise Warning(p[2].readline())
    return r

include_dirs = [get_pg_config(d) for d in ("includedir", "pkgincludedir", "includedir-server")]

multicorn_utils_module = Extension('multicorn._utils',
        include_dirs=include_dirs,
        extra_compile_args = ['-shared'],
        sources=['src/utils.c'])

setup(
 name='multicorn',
 version='__VERSION__',
 author='Kozea',
 license='Postgresql',
 package_dir={'': 'python'},
 packages=['multicorn', 'multicorn.fsfdw'],
 ext_modules = [multicorn_utils_module]
)
