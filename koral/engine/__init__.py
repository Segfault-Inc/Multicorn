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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
Engine
======

Engine base class.

"""

import abc
import os.path


class BaseEngine(object):
    """Abstract class for all template engine adaptators in Koral.

    Subclasses must override :method:`render`.

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
