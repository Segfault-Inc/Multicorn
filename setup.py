import subprocess
import os
import re
import shutil
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

class CMakeExtension(Extension):
    def __init__(self, name):
        Extension.__init__(self, name, sources=[])

class CMakeBuild(build_ext):
    def build_extension(self, ext):
        if os.name != 'nt':
            build_ext.build_extension(self, ext)
            return

        os.makedirs(self.build_temp, exist_ok=True)
        cmd = self.get_finalized_command('build_py').build_lib
        self.write_stub(cmd, ext)

        cfg = 'Debug' if self.debug else 'Release'
        subprocess.check_call([
            'cmake',
            '--build', '.',
            '--config', cfg
            ])
        shutil.copy(cfg + "/multicorn.dll", self.get_ext_fullpath(ext.name))


if os.name == 'nt':
    multicorn_utils_module = CMakeExtension('multicorn._utils')
else:
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


def find_version():
    pattern = re.compile(r"default_version\s*=\s*'([^']+)'")
    for i, line in enumerate(open('multicorn.control')):
        for match in re.finditer(pattern, line):
            return match.group(1)
    return '__VERSION__'

NAME = 'multicorn'

pgversion = os.getenv('PGVERSION')
if pgversion:
    NAME += '-pg' + pgversion

setup(
 name=NAME,
 version=find_version(),
 author='Kozea',
 license='Postgresql',
 package_dir={'': 'python'},
 packages=['multicorn', 'multicorn.fsfdw'],
 ext_modules = [multicorn_utils_module],
 cmdclass={
   'build_ext': CMakeBuild
 }
)
