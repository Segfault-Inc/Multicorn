from multicorn.property import Property
from multicorn.item import Item


class AbstractAccessPoint(object):

    ItemClass = Item
    
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
        
    def __init__(self, name, properties):
        self.name = name
        self.properties = dict(
            # If prop is not a Property already, assume it is the type
            (name, (prop if isinstance(prop, Property) else Property(prop)))
            for name, prop in properties.iteritems())
        self.metadata = None

    def bind(self, metadata):
        """
        Bind this access point to a Metadata.
        An access point can only be bound once.
        """
        if self.metadata is None:
            self.metadata = metadata
        else:
            raise RuntimeError('This access point is already bound.')
    
    def create(self, properties=None, lazy_loaders=None):
        """Create and return a new item."""
        properties = properties or {}
        lazy_loaders = lazy_loaders or {}
        item = self.ItemClass(self, properties, lazy_loaders)
        item.modified = True
        item.saved = False
        return item

