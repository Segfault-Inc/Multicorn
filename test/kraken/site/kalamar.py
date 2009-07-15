
from kraken.utils import Response

def handle_request(request, remaining_path):
    result = request.kalamar_site.search(u'fs_vorbis_messed_up', remaining_path)
    return Response(u'\n'.join(sorted(
        repr(dict(
            (key, item.properties[key])
            for key in (u'genre', u'artiste', u'album', u'titre')
        ))
        for item in result
    )))
    
