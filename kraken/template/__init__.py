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
import collections
import os
import re


def find_template(path, engines, template_root):
    """Get a template corresponding to ``path``.

    Return a named tuple ``(template_name, extension, engine)``.

    TODO: explain how the template is found

    """
    template_suffix_re = ur"\.(.+)\.(%s)$" % u"|".join(
        re.escape(engine) for engine in engines)

    searches = [(path, u"index")]
    # If path is empty (ie. path is u"" or u"/")
    # there is no path_parts[-1]
    if path:
        searches.append((os.path.dirname(path), os.path.basename(path)))

    Template = collections.namedtuple(
        "Template", ("template_name", "extension", "engine"))
    for dirname, basename in searches:
        abs_dirname = os.path.join(template_root, dirname)
        if os.path.isdir(abs_dirname):
            for name in os.listdir(abs_dirname):
                match = re.match(
                    re.escape(basename) + template_suffix_re, name)
                if match:
                    template_name = u"/".join(
                        dirname.split(os.path.sep) + [name])
                    extension = match.group(1)
                    engine = match.group(2)
                    return Template(template_name, extension, engine)


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


for name, engine in (
    ("jinja2", Jinja2Engine), ("genshi", GenshiEngine), ("mako", MakoEngine)):
    try:
        engine("/nonexistent-path")
    except ImportError:
        # Not installed/available
        pass
    else:
        BUILTIN_ENGINES[name] = engine
