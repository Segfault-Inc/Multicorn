# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Item
====

Base classes to create multicorn items.

"""

import abc
from collections import namedtuple, Mapping, MutableMapping


# Identity and *MultiMapping do not need an __init__ method
# pylint: disable=W0232

class Identity(namedtuple("Identity", "access_point, conditions")):
    """Simple class identifying items.

    :param access_point: The access point name of the item.
    :param conditions: A dict of conditions identifying the item.

    >>> identity = Identity("ap_name", {"id": 1})
    >>> identity.access_point
    'ap_name'
    >>> identity.conditions
    {'id': 1}

    :class:`Identity` manages equality between equivalent items.

    >>> identity2 = Identity("ap_name", {"id": 1})
    >>> identity == identity2
    True

    """


class MultiMapping(Mapping):
    """A Mapping where each key as associated to multiple values.
    
    Stored values are actually tuples, but :meth:`__getitem__` only gives
    the first element of that tuple.
    
    To access the underlying tuples, use :meth:`getlist`.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getlist(self, key):
        """Get the tuple of values associated to ``key``."""
        raise NotImplementedError

    def __getitem__(self, key):
        """Get the first value of the tuple of values associated to ``key``."""
        return self.getlist(key)[0]


class MutableMultiMapping(MultiMapping, MutableMapping):
    """A mutable MultiMapping.
    
    Stored values are actually tuples, but :meth:`__getitem__` only gives
    the first element of that tuple.
    
    To access the underlying tuples, use :meth:`getlist` and :meth:`setlist`.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def setlist(self, key, values):
        """Set the ``values`` tuple of values associated to ``key``."""
        raise NotImplementedError

    def __setitem__(self, key, value):
        """Set ``(value,)`` as the tuple of values associated to ``key``."""
        self.setlist(key, (value,))
    
    # The self argument is positional-only, in *args. This allows the method
    # to take a keyword argument named `self` without conflict.
    # See http://bugs.python.org/issue9137
    def update(*args, **kwargs):
        """Set values of the given mapping to the current mapping.

        The given arguments can be ``key=value`` named arguments, a regular
        mapping, a :class:`MultiMapping`, or an iterable of ``(key, value)``
        couples.

        """
        if len(args) > 2:
            raise TypeError("update() takes at most 2 positional "
                            "arguments ({} given)".format(len(args)))
        elif not args:
            raise TypeError("update() takes at least 1 argument (0 given)")
        self = args[0]
        other = args[1] if len(args) >= 2 else ()

        if isinstance(other, MultiMapping):
            # We have a MultiMapping object with a getlist method
            # pylint: disable=E1103
            for key in other:
                self.setlist(key, other.getlist(key))
            # pylint: enable=E1103
            other = ()
        super(MutableMultiMapping, self).update(other, **kwargs)

# pylint: enable=W0232


class MultiDict(MutableMultiMapping):
    """Concrete subclass of :class:`MutableMultiMapping` based on a dict.

    >>> multidict = MultiDict({"a": 1})
    >>> multidict["a"]
    1
    >>> multidict.getlist("a")
    (1,)
    >>> multidict.setlist("a", (1, "eggs", "spam"))
    >>> multidict["a"]
    1
    >>> multidict.getlist("a")
    (1, 'eggs', 'spam')
    >>> multidict["a"] = ("a", "b", "c")
    >>> multidict["a"]
    ('a', 'b', 'c')
    >>> multidict.getlist("a")
    (('a', 'b', 'c'),)
    >>> "a" in multidict
    True
    >>> del multidict["a"]
    >>> "a" in multidict
    False

    """
    def __init__(self, inital=()):
        self.__data = {}
        self.update(inital)
        
    def getlist(self, key):
        return self.__data[key]
    
    def setlist(self, key, values):
        self.__data[key] = tuple(values)

    def __delitem__(self, key):
        del self.__data[key]

    def __iter__(self):
        return iter(self.__data)

    def __len__(self):
        return len(self.__data)


class AbstractItem(MutableMultiMapping):
    """Abstract base class for Item-likes.

    :param access_point: The :class:`AccessPoint` where this item came from.

    """
    def __init__(self, access_point):
        self.access_point = access_point
        # An item is usually saved. If it isn't, it's because it has just been
        # created, and the access point is responsible for setting the flag to
        # ``False``.
        self.saved = False

    @abc.abstractmethod    
    def getlist(self, key):
        raise NotImplementedError
    
    @abc.abstractmethod
    def setlist(self, key, values):
        raise NotImplementedError

    def __delitem__(self, key):
        raise TypeError("%s object doesn't support item deletion." %
            self.__class__.__name__)

    def __iter__(self):
        return iter(self.access_point.properties)

    def __len__(self):
        return len(self.access_point.properties)

    def __contains__(self, key):
        # Mutable’s default implementation is correct
        # but based on __getitem__ which may needlessly call a lazy loader.
        return key in self.access_point.properties

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return self.reference_repr()

    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        names = (prop.name for prop in self.access_point.identity_properties)
        return Identity(
            self.access_point.name, dict((name, self[name]) for name in names))

    def __eq__(self, other):
        # __eq__ is required because MutableMultiMapping.__eq__ relies on
        # getlist that relies on MutableMultiMapping.__eq__
        return isinstance(other, AbstractItem) \
            and other.identity == self.identity

    def __cmp__(self, other):
        # TODO: test this method
        if isinstance(other, AbstractItem):
            if self.identity == other.identity:
                return 0
            elif self.identity > other.identity:
                return 1
        return -1

    def __hash__(self):
        return hash((self.access_point.name,
            frozenset((prop.name, self[prop.name])
                for prop in self.access_point.identity_properties)))

    def __str__(self):
        return str(self.reference_repr())

    def __unicode__(self):
        return self.reference_repr()

    def __bytes__(self):
        return bytes(self.reference_repr())

    def save(self):
        """Save the item."""
        self.access_point.save(self)

    def delete(self):
        """Delete the item."""
        self.access_point.delete(self)

    def reference_repr(self):
        """Return an unicode representation suitable for textual storage."""
        representations = []
        for prop in self.access_point.identity_properties:
            value = self[prop.name]
            if prop.type == Item:
                value = value.reference_repr()
            representations.append(unicode(value))
        return unicode("/".join(representations))

class Item(AbstractItem):
    """Item base class.

    :param access_point: The :class:`AccessPoint` where this item came from.
    :param properties: A :class:`Mapping` of initial values for this item’s
        properties. May be a :class:`MultiMapping` to have multiple values for
        a given property.
    :param lazy_loaders: A :class:`Mapping` of callable "loaders" for lazy
        properties. These callable should return a tuple of values.  When
        loading a property is expensive, this allows to only load it when it’s
        needed. When you have only one value, wrap it in a tuple like this:
        `(value,)`

    Every property defined in the access point must be given in one of
    ``properties`` or ``lazy_loaders``, but not both.

    """
    def __init__(self, access_point, properties=(), lazy_loaders=()):
        super(Item, self).__init__(access_point)
        given_keys = set(properties)
        lazy_keys = set(lazy_loaders)
        mandatory_keys = set((key for (key, prop)
            in access_point.properties.items()
            if prop.mandatory and not prop.auto))
        ap_keys = set(access_point.properties)

        missing_keys = mandatory_keys - given_keys - lazy_keys
        if missing_keys:
            raise ValueError("Properties %r are neither given nor lazy."
                             % (tuple(missing_keys),))
        intersection = given_keys & lazy_keys
        if intersection:
            raise ValueError("Properties %r are both given and lazy."
                             % (tuple(intersection),))
        extra = given_keys - ap_keys
        if extra:
            raise ValueError("Unexpected given properties: %r"
                             % (tuple(extra),))
        extra = lazy_keys - ap_keys
        if extra:
            raise ValueError("Unexpected lazy properties: %r"
                             % (tuple(extra),))

        given_properties = MultiDict(properties)
        self._loaded_properties = MultiDict()
        for key in given_properties:
            cast_value = self.access_point.properties[key].cast(
                given_properties.getlist(key))
            self._loaded_properties.setlist(key, cast_value)
        self._lazy_loaders = dict(lazy_loaders)
        self.modified = False

    def getlist(self, key):
        try:
            return self._loaded_properties.getlist(key)
        except KeyError:
            # KeyError (again) is expected here for keys not in
            # self.access_point.properties
            loader = self._lazy_loaders[key]
            values = loader(self)
            # Lazy loaders must return a tuple. To return a single value, wrap
            # it in a tuple: (value,).
            assert isinstance(values, tuple)
            self._loaded_properties.setlist(key, values)
            del self._lazy_loaders[key]
            return values

    def setlist(self, key, values):
        if key not in self:
            raise KeyError("%s object doesn't support adding new keys." %
                self.__class__.__name__)
        if key in (idprop.name for idprop
                in self.access_point.identity_properties) and self.saved:
            raise KeyError("Can not modify identity property %r." % key)

        self.modified = True
        values = self.access_point.properties[key].cast(values)
        self._loaded_properties.setlist(key, values)

        if key in self._lazy_loaders:
            del self._lazy_loaders[key]

    def save(self):
        """Save the item."""
        if self.modified:
            super(Item, self).save()
            self.modified = False


class ItemWrapper(AbstractItem):
    """Item wrapping another item.

    :param wrapped_item: Item wrapped in the current item.
    :param access_point: Access point where the current item is stored.

    """
    def __init__(self, access_point, wrapped_item):
        super(ItemWrapper, self).__init__(access_point)
        self.access_point = access_point
        self.wrapped_item = wrapped_item

    def getlist(self, key):
        return self.wrapped_item.getlist(key)

    def setlist(self, key, values):
        return self.wrapped_item.setlist(key, values)

    def __getattr__(self, name):
        """Default to underlying item for all other methods and attributes."""
        return getattr(self.wrapped_item, name)


class ItemStub(AbstractItem):
    """Item stub containing only the identity properties initially."""
    def __init__(self, access_point, identity_props):
        super(ItemStub, self).__init__(access_point)
        self.__item = None
        self.identity_props = identity_props

    @property
    def item(self):
        """Underlying item only loaded if needed."""
        if self.__item is None:
            site = self.access_point.site
            self.__item = site.open(self.access_point.name, self.identity_props)
        return self.__item

    def getlist(self, key):
        if key in self.identity_props:
            return (self.identity_props[key],)
        else:
            return self.item.getlist(key)

    def setlist(self, key, values):
        return self.item.setlist(key, values)

    def __getattr__(self, name):
        return getattr(self.item, name)
