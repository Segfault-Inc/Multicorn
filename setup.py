from setuptools import setup, find_packages, Extension

multicorn_utils_module = Extension('multicorn._utils',
        include_dirs=['/usr/include/postgresql/', '/usr/include/postgresql/server', '/usr/include/postgresql/internal/'],
        extra_compile_args = ['-shared'],
        sources=['src/utils.c'])

setup(
 name='multicorn',
 version='__VERSION__',
 author='Kozea',
 install_requires=['boto',],
 license='Postgresql',
 package_dir={'': 'python'},
 packages=['multicorn', 'multicorn.fsfdw'],
 ext_modules = [multicorn_utils_module],
)
