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
# along with Koral.  If not, see <http://www.gnu.org/licenses/>.

def recursive_subclasses(class_):
    """Return all "class_" subclasses recursively."""
    yield class_
    for subclass in class_.__subclasses__():
        for sub_subclass in recursive_subclasses(subclass):
            yield sub_subclass

def indent(text, indent_level):
    r"""
    >>> indent(u'\n\nLorem ipsum dolor sit amet.\n'
    ...        u'  Maecenas malesuada iaculis luctus.\n\n', 2)
    u'  Lorem ipsum dolor sit amet.\n    Maecenas malesuada iaculis luctus.'
    """
    lines = text.splitlines()

    # Strip off trailing and leading blank lines:
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and not lines[0].strip():
        lines.pop(0)
        
    indent_string = u' ' * indent_level
    return u'\n'.join(indent_string + line for line in lines)
