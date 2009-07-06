
# Pull setuptools from the web if needed
from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

setup(
    name = 'Dyko"',
    version = '0.1',
    packages = find_packages(
        exclude=['*._test', '*._test.*', 'test.*', 'test']
    ),
    scripts = ['dyko.py'],
    
    package_dir = {
        'kalamar': 'kalamar',
        'koral': 'koral',
        'kraken': 'kraken'
    },

    package_data = {
        '': ['AUTHORS'],
        'doc': ['*.txt', '*.rst']
    },

    install_requires = {
        'Werkzeug': ['werkzeug>=0.5']
    },
    
    extras_require = {
        'Genshi': ['genshi>=0.5'],
        'Jinja2': ['jinja2>=2.0'],
        'Ogg/Vorbis': ['mutagen>=1.16']
    },

    # metadata for upload to PyPI
    author = "Kozea S.A.R.L.",
    author_email = "guillaume.ayoub@kozea.fr",
    description = "This is a light and flexible web framework in full python.",
    license = "GPL",
    keywords = "web framework database",
    url = "http://dyko.org",
    
    entry_points = {
        'console_scripts': ['dyko = dyko:main']
    },
    
    zip_safe=False

)

