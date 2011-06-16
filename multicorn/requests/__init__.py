from . import requests
from .requests import as_request # To be imported form here


CONTEXT = requests.ContextRequest()


def literal(obj):
    """
    Wrap any Python object into a request that represents that object.
    """
    return requests.LiteralRequest(obj)
