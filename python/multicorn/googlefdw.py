"""
A foreign data wrapper for performing google searches.

"""

from . import ForeignDataWrapper

import json
import urllib


def google(search):
    """Retrieves results from google using the json api"""
    query = urllib.urlencode({'q': search})
    url = ('http://ajax.googleapis.com/ajax/'
           'services/search/web?v=1.0&%s' % query)
    response = urllib.urlopen(url)
    results = response.read()
    results = json.loads(results)
    data = results['responseData']
    hits = data['results']
    for hit in hits:
        yield {'url': hit['url'].encode("utf-8"),
               'title': hit["titleNoFormatting"].encode("utf-8"),
               'search': search.encode("utf-8")}


class GoogleFdw(ForeignDataWrapper):
    """A Google search foreign data wrapper.

    Parses the quals to find anything ressembling a search criteria, and
    returns the google search result for it.
    Available columns are: url, title, search.

    """

    def execute(self, quals, columns):
        if not quals:
            return ("No search specified",)
        for qual in quals:
            if qual.field_name == "search" or qual.operator == "=":
                return google(qual.value)
