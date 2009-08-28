

class CachedKalamarSite(object):
    """
    Wrapper for kalamar that caches results of the following methods:
        isearch, search, open, item_from_filenames
    All cached entries are removed when the following methods are called:
        save, remove
    Warning: arguments for cached methods must be hashable
    
    >>> class FakeKalamar(object):
    ...     y = 1
    ...     def search(self, x):
    ...         print 'search', x
    ...         return x + self.y
    ...     def save(self, y):
    ...         print 'save', y
    ...         self.y = y
    >>> kalamar = CachedKalamarSite(FakeKalamar())
    >>> kalamar.search(2)
    search 2
    3
    >>> kalamar.search(5)
    search 5
    6
    >>> kalamar.search(2) # result is cached: FakeKalamar.search is not called
    3
    >>> kalamar.save(-1)
    save -1
    >>> kalamar.search(2) # cache has been invalidated
    search 2
    1
    """
    def __init__(self, kalamar_site):
        self.kalamar_site = kalamar_site
        self._cache = {}
    
    def _cached_method(method_name):
        def _wrapper(self, *args, **kwargs):
            key = (method_name, args, tuple(sorted(kwargs.items())))
            try:
                # XXX all args and kwargs must be hashable
                # (use a tuples instead of lists)
                return self._cache[key]
            except KeyError:
                value = getattr(self.kalamar_site, method_name)(*args, **kwargs)
                self._cache[key] = value
            return value
        _wrapper.__name__ = method_name
        return _wrapper
    
    isearch = _cached_method('isearch')
    search = _cached_method('search')
    open = _cached_method('open')
    item_from_filename = _cached_method('item_from_filename')
    del _cached_method
    
    def save(self, *args, **kwargs):
        self._cache = {}
        return self.kalamar_site.save(*args, **kwargs)
        
    def remove(self, *args, **kwargs):
        self._cache = {}
        return self.kalamar_site.remove(*args, **kwargs)

    def __getattr__(self, name):
        """Proxy for other methods"""
        return getattr(self.kalamar_site, name)
    
