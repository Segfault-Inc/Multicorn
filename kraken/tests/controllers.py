from kraken.site import expose_template

@expose_template("/helloparam/<string:message>/")
def helloparam(request, message, **kwargs):
    return {"message" : message}

@expose_template("/methods/",methods=("GET",))
def getmethod(request, **kwargs):
    return {"message": "GET world"}

@expose_template("/methods/",methods=("POST",))
def postmethod(request, **kwargs):
    return {"message": "POST world"}

@expose_template()
def hello(request, **kwargs):
    return {'request' : request}

@expose_template("/another/template/<string:message>", template="helloparam")
def anothertemplate(request, message, **kwargs):
    return {'message': message}

@expose_template("/<string:hello>/message")
def weirdpath(request, hello, **kwargs):
    return {'message': hello} 

@expose_template("/")
def index(request, **kwargs):
    """This endpoint isn't actually tested, but ensures that the template is
    correctly found even on index"""
    return {}
