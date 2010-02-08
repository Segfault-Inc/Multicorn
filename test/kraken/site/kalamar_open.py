
from kraken.utils import Response

def handle_request(request, remaining_path):
    item = request.kalamar.open_or_404(u'fs_text_mixed', remaining_path)
    return Response(repr(dict(
        (key, item[key])
        for key in (u'genre', u'artiste', u'album', u'titre')
    )))
    
