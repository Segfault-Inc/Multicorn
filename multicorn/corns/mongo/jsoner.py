from bson import json_util
import json


def to_json(py):
    return json.dumps(py, default=json_util.default)


def from_json(json_):
    return json.loads(json_, object_hook=json_util.object_hook)
