from abc import ABCMeta, abstractmethod
from .request import RequestProperty, make_request_property
from .property import Property
import __builtin__


class base_func(object):

    __metaclass__ = ABCMeta

    def __init__(self, property):
        self.property = make_request_property(property)

    def return_property(self, properties):
        return properties.get(self.property.name, None)



class transform_func(base_func, RequestProperty):


    @property
    def child_property(self):
        if self.property.child_property:
            child = self._copy()
            child.property = self.property.child_property
            return child
        return None

    def _copy(self):
        return self.__class__(self.property)

    @property
    def name(self):
        return self.property.name

    def get_value(self, item):
        return self(self.property.get_value(item))


class base_aggregate(base_func):

    def initializer(self, properties):
        return properties[self.property.name].type()

    @abstractmethod
    def __call__(self, accumulator, value):
        """An aggregate function must be suitable to be called through a "reduce"
        call.
        """
        pass

class sum(base_aggregate):

    def __call__(self, accumulator, item):
        return accumulator + self.property.get_value(item)

class count(base_aggregate):

    def initializer(self, properties):
        return 0

    def return_property(self, properties):
        return Property(int)

    def __init__(self):
        pass

    def __call__(self, accumulator, value):
        return accumulator + 1

class max(base_aggregate):

    def initializer(self, property):
        return None

    def __call__(self, accumulator, item):
        if accumulator is None:
            return self.property.get_value(item)
        return __builtin__.max(accumulator, self.property.get_value(item))

class min(base_aggregate):

    def initializer(self, property):
        return None

    def __call__(self, accumulator, item):
        if accumulator is None:
            return self.property.get_value(item)
        return __builtin__.min(accumulator, self.property.get_value(item))

class slice(transform_func):

    def _copy(self):
        return self.__class__(self.property, [self.range.start, self.range.stop])

    def __init__(self, property_name, select_range):
        super(slice, self).__init__(property_name)
        if hasattr(select_range, "__iter__"):
            self.range = __builtin__.slice(*select_range)
        else:
            self.range= __builtin__.slice(select_range)

    def __call__(self, value):
        return value[self.range] if value else value

class coalesce(transform_func):

    def __init__(self, property_name, replacement):
        super(coalesce, self).__init__(property_name)
        self.replacement = replacement

    def _copy(self):
        return self.__class__(self.property, self.replacement)

    def __call__(self, value):
        if value is None:
            return self.replacement
        return value
