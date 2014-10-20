
try:
    unicode_ = unicode
except NameError:
    # Python3
    unicode_ = str

try:
    basestring_ = basestring
except NameError:
    # Python3
    basestring_ = str
