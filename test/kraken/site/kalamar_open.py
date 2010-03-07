
from kraken.utils import Response

def handle_request(request):
    kalamar_request = request.query_string.decode('utf8')
    item = request.kalamar.open_or_404(u'fs_text_mixed', kalamar_request)
    return Response(repr(dict(
        (key, item[key])
        for key in (u'genre', u'artiste', u'album', u'titre')
    )))
    
