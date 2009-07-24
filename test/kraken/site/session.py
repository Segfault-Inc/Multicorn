
from kraken.utils import Response

def handle_request(request, remaining_path):
    if remaining_path:
        request.session['test_session'] = remaining_path
    return Response(repr(request.session.get('test_session', u'(no value)')))
    
