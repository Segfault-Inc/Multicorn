# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from . import requests
from .requests import as_request # To be imported form here


CONTEXT = requests.ContextRequest()


def literal(obj):
    """
    Wrap any Python object into a request that represents that object.
    """
    return requests.LiteralRequest(obj)
