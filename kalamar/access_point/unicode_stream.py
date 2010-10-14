# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.


import copy
try:
    from cStringIO import StringIO as real_StringIO
    def StringIO(value):
        """cStringIO.StringIO objects are read-only when constructed with
        a parameter.  This constructor makes read-write objects.
        
        """
        io = real_StringIO()
        io.write(value)
        io.seek(0)
        return io
except ImportError:
    from StringIO import StringIO

from . import AccessPointWrapper, AccessPoint
from ..item import ItemWrapper, MultiDict
from ..request import And


class UnicodeStreamItem(ItemWrapper):
    def getlist(self, key):
        if key != self.access_point.stream_property_name:
            return self.wrapped_item.getlist(key)
        if hasattr(self, '_decoded_values'):
            return self._decoded_values
        self._decoded_values = self.access_point._decode(
            self.wrapped_item.getlist(key))
        return self._decoded_values
    
    def setlist(self, key, values):
        if key != self.access_point.stream_property_name:
            return self.wrapped_item.setlist(key, values)
        values = self.access_point.stream_property.cast(values)
        self._decoded_values = values
        self.wrapped_item.setlist(key, self.access_point._encode(values))


class UnicodeStream(AccessPointWrapper):
    """This access point wrapper reads and decode (as unicode) a stream property
    from the underlying items.

    """
    ItemWrapper = UnicodeStreamItem
    
    def __init__(self, wrapped_ap, stream_property_name, encoding):
        super(UnicodeStream, self).__init__(wrapped_ap)
        self.stream_property_name = stream_property_name
        self.encoding = encoding
        
        # Make a shallow copy of the property we override.
        self.stream_property = copy.copy(self.properties[stream_property_name])
        self.stream_property.type = unicode
        self.properties[stream_property_name] = self.stream_property
    
    def _decode(self, streams):
        return tuple(stream.read().decode(self.encoding) for stream in streams)
        
    def _encode(self, values):
        return tuple(StringIO(value.encode(self.encoding)) for value in values)
        
    def create(self, properties=None, lazy_loaders=None):
        properties = MultiDict(properties or {})
        try:
            values = properties.getlist(self.stream_property_name)
        except KeyError:
            pass
        else:
            values = self.stream_property.cast(values)
            properties.setlist(self.stream_property_name, self._encode(values))
        
        lazy_loaders = lazy_loaders or {}
        try:
            loader = lazy_loaders[self.stream_property_name]
        except KeyError:
            pass
        else:
            def new_loader():
                values = loader()
                values = self.stream_property.cast(values)
                return self._encode(values)
            lazy_loaders[self.stream_property_name] = new_loader
            
        return super(UnicodeStream, self).create(properties, lazy_loaders)

    def _split_request(self, request):
        # Constraints here:
        #  * `And(request_for_wrapped_ap, remaining_request)` 
        #    must be equivalent to `request`.
        #  * request_for_wrapped_ap must not contain conditions about the
        #    converted property. (ie. self.stream_property_name)
        
        # TODO: decompose better, at least for simple cases.
        # eg. {'a': 1, 'b': 2} should decompose to {'a': 1} and {'b': 2}
        # (assuming self.stream_property_name == 'b')
        request_for_wrapped_ap = And()
        remaining_request = request
        return request_for_wrapped_ap, remaining_request
        
    def search(self, request):
        request_for_wrapped_ap, remaining_request = self._split_request(request)
        for item in super(UnicodeStream, self).search(request_for_wrapped_ap):
            if remaining_request.test(item):
                yield item

    def delete_many(self, request):
        request_for_wrapped_ap, remaining_request = self._split_request(request)
        if remaining_request == And():
            # request is not about self.stream_property_name, we can safely
            # pass it to the underlying access point’s delete_many
            super(UnicodeStream, self).delete_many(request)
        else:
            # use the "stupid" but safe default implementation
            # based on search+delete
            AccessPoint.delete_many(self, request)

