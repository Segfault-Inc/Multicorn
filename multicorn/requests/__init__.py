from . import requests
from .requests import as_request


CONTEXT = requests.ContextRequest()


def literal(obj):
    """
    Wrap any Python object into a request that represents that object.
    """
    return requests.LiteralRequest(obj)
