========
Plankton
========

Reçoit :

- dictionnaire de styles (un style pour tous les formats où on peut définir
  un style)
- chaîne unicode ou binaire (selon le format)
- mimetype in
- mimetype out

Sort :

- une chaîne de caractère unicode ou binaire

===
API
===

::
  
  class site:
    converters = {}
    def __init__(…, path_to_root)
  
  usage : converter = MySite.get_converter(mimetype_in, mimetype_out)
          result = converter(content_string, themes_dict)
