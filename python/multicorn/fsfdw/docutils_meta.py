"""
Use low-level docutils API to extract metadata from ReStructuredText files.
"""
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from threading import Lock
from functools import wraps
from os.path import getmtime

from docutils.core import publish_doctree


def extract_meta(filename):
    """Read meta-data from a reStructuredText file and return a dict.

    The 'title' and 'subtitle' keys are special-cased, but other keys
    are read from the `docinfo` element.

    """
    with open(filename) as file_obj:
        content = file_obj.read()
    meta = {}
    for element in publish_doctree(content):
        if element.tagname in ('title', 'subtitle'):
            meta[element.tagname] = element.astext()
        elif element.tagname == 'docinfo':
            for field in element:
                if field.tagname == 'field':
                    name, body = field.children
                    meta[name.astext().lower()] = body.astext()
                else:
                    meta[field.tagname.lower()] = field.astext()
    return meta


def mtime_lru_cache(function, max_size=100):
    """File mtime-based least-recently-used cache.

    :param function:
        A function that takes a filename as its single parameter.
        The file should exist, and the function's return value should
        only depend on the contents of the file.

    Return a decorated function that caches at most the ``max_size`` value.
    Least recently used value are dropped first. Cached values are invalidated
    when the files's modification time changes.

    Inspired from functools.lru_cache, which only exists in Python 3.2+.

    """
    lock = Lock()  # OrderedDict isn't threadsafe
    cache = OrderedDict()  # ordered least recent to most recent

    @wraps(function)
    def wrapper(filename):
        mtime = getmtime(filename)
        with lock:
            if filename in cache:
                old_mtime, result = cache.pop(filename)
                if old_mtime == mtime:
                    # Move to the end
                    cache[filename] = old_mtime, result
                    return result
        result = function(filename)
        with lock:
            cache[filename] = mtime, result  # at the end
            if len(cache) > max_size:
                cache.popitem(last=False)
        return result
    return wrapper
