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
# along with Kraken.  If not, see <http://www.gnu.org/licenses/>.

"""
Tests for kraken controllers.

"""

from kraken.site import expose_template

# Tests don't use all controllers parameters
# pylint: disable=W0613

@expose_template("/helloparam/<string:message>/")
def helloparam(request, message, **kwargs):
    """Template with parameters."""
    return {"message": message}

@expose_template("/methods/", methods=("GET",))
def getmethod(request, **kwargs):
    """Template available by GET method."""
    return {"message": "GET world"}

@expose_template("/methods/", methods=("POST",))
def postmethod(request, **kwargs):
    """Template available by POST method."""
    return {"message": "POST world"}

@expose_template()
def hello(request, **kwargs):
    """Template automatically called ``hello`` from the function name."""
    return {"request": request}

@expose_template("/another/template/<string:message>", template="helloparam")
def anothertemplate(request, message, **kwargs):
    """Template available from another name."""
    return {"message": message}

@expose_template("/<string:hellostring>/message")
def weirdpath(request, hellostring, **kwargs):
    """Template with a weird path."""
    return {"message": hellostring}

@expose_template("/")
def index(request, **kwargs):
    """Index template with a ``/`` path.

    This endpoint isn't actually tested, but ensures that the template is
    correctly found even on index.

    """
    return {}

# pylint: enable=W0613
