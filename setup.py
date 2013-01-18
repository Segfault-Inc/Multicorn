from setuptools import setup, find_packages, Extension

multicorn_utils_module = Extension('multicorn._utils',
        include_dirs=['/usr/include/postgresql/', '/usr/include/postgresql/server', '/usr/include/postgresql/internal/', '/usr/pgsql-9.2/include/server'],
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
