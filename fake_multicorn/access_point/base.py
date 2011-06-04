
class AbstractAccessPoint(object):
    
    @classmethod
    def declarative(cls, class_):
        """
        Allow declarative instanciation, which in turn allows registration
        with a class decorator
        
            @metadata.register
            @SomeAccessPoint.declarative
            class pages:
                foo = 'bar'
        
        is the same as:
        
            pages = SomeAccessPoint(name='pages', foo='bar')
            metadata.register(pages)
        
        The declarative syntax may be more readable when arguments are many,
        long or deeply nested.
        """
        args = {'name': class_.__name__}
        args.update(
            (name, value) for name, value in vars(class_).iteritems()
            if not name.startswith('__'))
        return cls(**args)
        
    def __init__(self, name):
        self.name = name
        self.metadata = None

    def bind(self, metadata):
        if self.metadata is None:
            self.metadata = metadata
        else:
            raise RuntimeError('This access point is already bound.')
        
