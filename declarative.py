
class DeclarativeMixin(object):
    """
    A subclass of this class:

        >>> class MyDict(DeclarativeMixin, dict):
        ...     pass # do stuff
        
    can be instanciated by specifying keywords arguments as follows.
    These keywords arguments can use the same syntax themselves but no
    positional arguments can be given.
    
        >>> @MyDict.declarative
        ... class some_object1:
        ...     hello = 'World'
        ...     
        ...     @MyDict.declarative
        ...     class recursion:
        ...         yay = True
    
    This syntax would be the same as
    
        >>> some_object2 == MyDict(hello='World', recursion=MyDict(yay=True))
        >>> some_object2 == some_object1
        True
    
    but can be more readable when arguments are many, long and/or
    deeply nested.

    """
    
    @classmethod
    def declarative(cls, class_):
        return cls(**dict(
            (name, value) for name, value in vars(class_).iteritems()
            if not name.startswith('__')))


if __name__ == '__main__':
    import doctest
    doctest.testmod()
