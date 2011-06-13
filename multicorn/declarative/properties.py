from ..corns.extensers.typeextenser import TypeExtenser


class Property():
    _wrapper = None

    def __init__(self, property=None, **kwargs):
        self.wrapped_property = property
        self.kwargs = kwargs

    @property
    def depth(self):
        if self.wrapped_property:
            return self.wrapped_property.depth + 1
        else:
            return 0


class TypeProperty(Property):
    _wrapper = TypeExtenser


class DateProperty(TypeProperty):
    pass


class TextProperty(TypeProperty):
    pass
