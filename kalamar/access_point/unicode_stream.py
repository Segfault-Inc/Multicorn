# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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

"""
Unicode Stream
==============

Wrapper access point reading and decoding, as unicode, a stream property from
the underlying items.

"""

from copy import copy
from StringIO import StringIO

from . import AccessPointWrapper, AccessPoint
from ..item import ItemWrapper, MultiDict
from ..request import And


class UnicodeStreamItem(ItemWrapper):
    """Unicode stream item."""
    # Unicode items can access access point private methods
    # pylint: disable=W0212
    def getlist(self, key):
        if key != self.access_point.stream_property_name:
            return self.wrapped_item.getlist(key)
        return self.access_point._decode(self.wrapped_item.getlist(key))
    
    def setlist(self, key, values):
        if key != self.access_point.stream_property_name:
            self.wrapped_item.setlist(key, values)
        else:
            values = self.access_point.stream_property.cast(values)
            self.wrapped_item.setlist(key, self.access_point._encode(values))
    # pylint: enable=W0212


class UnicodeStream(AccessPointWrapper):
    """Unicode stream access point."""
    ItemWrapper = UnicodeStreamItem
    
    def __init__(self, wrapped_ap, stream_property_name, encoding):
        super(UnicodeStream, self).__init__(wrapped_ap)
        self.stream_property_name = stream_property_name
        self.encoding = encoding
        
        # Make a shallow copy of the property we override.
        self.stream_property = copy(self.properties[stream_property_name])
        self.stream_property.type = unicode
        self.properties[stream_property_name] = self.stream_property
    
    def _decode(self, streams):
        """Get decoded values from ``streams``."""
        return tuple(stream.read().decode(self.encoding) for stream in streams)
        
    def _encode(self, values):
        """Get encoded streams from ``values``."""
        return tuple(StringIO(value.encode(self.encoding)) for value in values)
        
    def create(self, properties=None, lazy_loaders=None):
        properties = MultiDict(properties or {})

        if self.stream_property_name in properties:
            values = properties.getlist(self.stream_property_name)
            values = self.stream_property.cast(values)
            properties.setlist(self.stream_property_name, self._encode(values))

        if lazy_loaders and self.stream_property_name in lazy_loaders:
            loader = lazy_loaders[self.stream_property_name]
            lazy_loaders[self.stream_property_name] = \
                lambda: self.stream_property.cast(self._encode(loader()))

        return super(UnicodeStream, self).create(properties, lazy_loaders)

    def _split_request(self, request):
        """Split ``request`` into ``non_stream, stream`` properties."""
        # Constraints here:
        #
        # - ``And(request_for_wrapped_ap, remaining_request)`` must be
        #   equivalent to `request`.
        # - ``request_for_wrapped_ap`` must not contain conditions about the
        #   converted property, ie. ``self.stream_property_name``.
        
        # TODO: decompose better, at least for simple cases.
        # eg. {"a": 1, "b": 2} should decompose to {"a": 1} and {"b": 2}
        # (assuming ``self.stream_property_name`` == "b")
        request_for_wrapped_ap = And()
        remaining_request = request
        return request_for_wrapped_ap, remaining_request
        
    def search(self, request):
        request_for_wrapped_ap, remaining_request = self._split_request(request)
        for item in super(UnicodeStream, self).search(request_for_wrapped_ap):
            if remaining_request.test(item):
                yield item

    def delete_many(self, request):
        remaining_request = self._split_request(request)[1]
        if remaining_request == And():
            # ``request`` is not about ``self.stream_property_name``, we can
            # safely pass it to the underlying access point’s ``delete_many``
            super(UnicodeStream, self).delete_many(request)
        else:
            # Use the "stupid" but safe default implementation
            # based on ``search`` + ``delete``
            AccessPoint.delete_many(self, request)
