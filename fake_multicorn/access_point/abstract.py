from multicorn.property import Property
from multicorn.item import Item

from .. import queries


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
        
    def __init__(self, name, properties, identity_properties):
        self.name = name
        self.metadata = None

        self.properties = {}
        for name, prop in properties.iteritems():
            if not isinstance(prop, Property):
                # Assume it's just a type.
                prop = Property(prop)
            self.properties[name] = prop
            prop.bind(self, name)
            if name in identity_properties:
                prop.mandatory = True
                prop.identity = True

        self.identity_properties = []
        for name in identity_properties:
            self.identity_properties.append(self.properties[name])


    def bind(self, metadata):
        """
        Bind this access point to a Metadata.
        An access point can only be bound once.
        """
        if self.metadata is None:
            self.metadata = metadata
        else:
            raise RuntimeError('This access point is already bound.')
    
    def create(self, properties=None, lazy_loaders=None, save=True):
        """Create and return a new item."""
        properties = properties or {}
        lazy_loaders = lazy_loaders or {}
        item = self.ItemClass(self, properties, lazy_loaders)
        item.modified = True
        item.saved = False
        if save:
            self.save(item)
        return item
    
    # Minimal API for concrete access points
    
    def save(self, item):
        """Return an iterable of all items in this access points."""
        raise NotImplementedError
    
    def _all(self):
        """Return an iterable of all items in this access points."""
        raise NotImplementedError

    # Can be overridden to optimize
    
    def search(self, query=None):
        """Execute the given query and return an iterable of items."""
        if query is None:
            # The empty query does nothing and gives all items.
            query = queries.Query
        return queries.execute(self._all(), query)

