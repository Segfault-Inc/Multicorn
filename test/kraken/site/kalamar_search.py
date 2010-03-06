
from kraken.utils import Response

def handle_request(request):
    kalamar_request = request.query_string.decode('utf8')
    result = request.kalamar.search(u'fs_text_mixed', kalamar_request)
    return Response(u'\n'.join(sorted(
        repr(dict(
            (key, item[key])
            for key in (u'genre', u'artiste', u'album', u'titre')
        ))
        for item in result
    )))
    
