def render(request, **kwargs):
    if request.query_string:
        request.session["test_session"] = request.query_string
    return request.session.get("test_session", u"(no value)")
