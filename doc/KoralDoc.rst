=====
Koral
=====

Reçoit :

- modificateur
- dictionnaire de paramètres
  - _css (liste d'url)
  - _javascript (liste d'url)
  - _lang (fr-fr, en-us …)

- lang (comme la précédente «lang»)
- type de moteur (kid, py)

Sort :

- une chaîne de caractère unicode

===
API
===

class site:
  engines = {}
  def __init__(…, path_to_root)

appel : MonSite.engines["kid"](template_name, values, lang, modifiers)
