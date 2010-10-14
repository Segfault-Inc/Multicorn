from werkzeug import Response


def handle_request(request):
    return Response("""
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
        Proin sit amet est elementum lorem lacinia vulputate. Ut ut massa 
        nibh. Vestibulum ante velit, ornare id porta sed, semper in dui. 
        Duis dictum elit sit amet felis cursus sit amet rhoncus sem tincidunt.
        Nulla ac est erat. Cum sociis natoque penatibus et magnis dis 
        parturient montes, nascetur ridiculus mus. Curabitur sodales tincidunt
        ipsum a hendrerit. Etiam aliquam ipsum ut leo posuere vitae rutrum 
        diam venenatis. Fusce ornare tristique odio sit amet dapibus. In hac
        habitasse platea dictumst. Proin condimentum rutrum laoreet. Etiam 
        non neque sollicitudin diam viverra cursus vitae eget risus. 
        Nulla vehicula, eros eget sagittis venenatis, enim dolor laoreet 
        risus, at sodales turpis elit et massa.
    """)

