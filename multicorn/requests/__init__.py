# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from . import requests
from .requests import as_request, literal # To be imported form here
from .requests import CaseRequest, WhenRequest, ARGUMENT_NOT_GIVEN

CONTEXT = requests.ContextRequest()


def when(condition, result):
    return WhenRequest(condition, result)


def case(*args):
    assert all(isinstance(arg, WhenRequest) for arg in args[:-1])
    if not isinstance(args[-1], WhenRequest):
        return CaseRequest(args[:-1], args[-1])

    return CaseRequest(args, ARGUMENT_NOT_GIVEN)
