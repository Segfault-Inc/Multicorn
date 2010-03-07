
from kraken.utils import Response

def handle_request(request):
    if request.query_string:
        request.session['test_session'] = request.query_string
    return Response(request.session.get('test_session', u'(no value)'))
    
