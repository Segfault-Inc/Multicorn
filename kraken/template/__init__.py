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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
Template Engine
===============

Template engine manager base class.

"""

import abc
import os
import re
from collections import namedtuple


# A named tuple has no real __init__ method
# pylint: disable=W0232
class Template(namedtuple("Template", ("name", "extension", "engine"))):
    """Template named tuple with ``name``, ``extension`` and ``engine``."""
# pylint: enable=W0232


def find_template(path, engines, template_root):
    """Get a :class:`Template` corresponding to ``path``.

    A simple algorithm is used to find the matching template:

    1. Find a file named ``<root>/<path>.<mimetype>.<engine>``. If such a file
       exists, choose this file, else;
    2. Find a file named ``<root>/<path>/index.<mimetype>.<engine>``. If such a
       file exists, choose this file, else;
    3. Return ``None``.

    At each step, ``engine`` must be in the list of ``engines``. If more than
    one file matching the filename is present (for example, same name with a
    different engine or a different mimetype), one file is randomly chosen.

    """
    template_suffix_re = ur"\.(.+)\.(%s)$" % u"|".join(
        re.escape(engine) for engine in engines)

    searches = [(path, u"index")]
    # If path is empty (ie. path is u"" or u"/")
    # there is no path_parts[-1]
    if path:
        searches.append((os.path.dirname(path), os.path.basename(path)))

    for dirname, basename in searches:
        abs_dirname = os.path.join(template_root, dirname)
        if os.path.isdir(abs_dirname):
            for name in os.listdir(abs_dirname):
                match = re.match(
                    re.escape(basename) + template_suffix_re, name)
                if match:
                    name = u"/".join(dirname.split(os.path.sep) + [name])
                    extension = match.group(1)
                    engine = match.group(2)
                    return Template(name, extension, engine)


class BaseEngine(object):
    """Abstract class for all template engine adaptators in Koral.

    Subclasses must override :meth:`render`.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, path_to_root):
        """Template engine initialisation."""
        self.path_to_root = path_to_root

    def _build_filename(self, template_name):
        """Convert a slash-separated template name to an absolute filename."""
        parts = (part for part in template_name.split(u"/")
            if part and part != u"..")
        return os.path.join(self.path_to_root, *parts)

    @abc.abstractmethod
    def render(self, template_name, values, lang, modifiers):
        """Render ``template_name`` with the ``values`` dict, return unicode.

        This method has to be overriden.

        :param template_name: Path to the template file used.
        :param values: Mapping of values used by the template.
        :param lang: Lang code like "en-us" or "fr"
        :param modifiers: Theming modifiers that can be specific to each
            template engine.

        """
        raise NotImplementedError


from .python import PythonEngine
from .str_format import StrFormatEngine
from .jinja2_ import Jinja2Engine
from .genshi_ import GenshiEngine
from .mako_ import MakoEngine


#: Mapping of built-in Koral engines available.
BUILTIN_ENGINES = {"py": PythonEngine, "str-format": StrFormatEngine}


for engine_name, engine_class in (
    ("jinja2", Jinja2Engine), ("genshi", GenshiEngine), ("mako", MakoEngine)):
    try:
        engine_class("/nonexistent-path")
    except ImportError: # pragma: no cover
        # Not installed/available
        import sys
        print(
            "WARNING: The %s template engine is not available." % engine_name,
            file=sys.stderr)
    else:
        BUILTIN_ENGINES[engine_name] = engine_class
