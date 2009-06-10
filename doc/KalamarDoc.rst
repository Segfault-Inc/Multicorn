====================
Description générale
====================

Kalamar est une couche d'abstraction permettant donnant une interface de
programmation unifiée pour accéder à des données présentes sous différents
supports et sous différents formats.

Kalamar se compose :

- D'un répartisseur configurable qui répartit les requêtes entre les différents modules.
- De plusieurs modules (« accesseurs ») propres à chaque support de données
  (p.ex. : MySQL, gestionnaire de fichier, annuaire LDAP, gestionnaire de
  version…). Ces modules génèrent éventuellement des propriétés.
- De plusieurs modules (« extracteurs ») propres à chaque format de données et
  permettant d'extraire des propriétés (p.ex. : image, MPEG, texte, ReST,
  HTML, RAW…). À un même « format » au sens usuel, on peut attribuer plusieurs
  « formats » différents au sens Kalamar, c'est à dire différents extracteurs
  (un document XML peut être considéré comme un document texte par exemple, ou
  comme une image si c'est du SVG).

Les formats donnés sont divisés conceptuellement en deux catégories : les
« atomes » et les « capsules ». Atomes et capsules sont tous des « items ».

- Les atomes sont des éléments indivisibles d'information. Ils possèdent des
  propriétés et un contenu (qui est en fait une propriétés particulière) mais
  ne contiennent pas d'autres atomes.
- Les capsules sont des listes ordonnées d'items. Elles possèdent des
  propriétés mais pas de contenu (on peut tricher et leur en faire contenir si
  on veut programmer salement, c'est à vos risques et périls…).

Ainsi, un atome image et une capsule image ne sont pas le même format (et
n'auront pas le même nom, p.ex. ImageAtom et ImageCapsule). En effet, l'atome
donne accès aux données brutes de l'image (et vous vous débrouillez avec) alors
que la capsule peut donner accès à chaque pixel.

Deux propriétés clés des items sont leur support et leur format. Un item aura
toujours au moins ces deux propriétés. Le support est défini par l'accesseur
utilisé pour cet item, le format par son extracteur. L'extracteur définit aussi
si l'item est un atome ou une capsule. Ces trois points sont importants comme
nous allons le voir tout de suite.

Les items sont accessibles via des requêtes sous forme de chaîne de caractères
(**TODO** : spécifier de façon exacte la syntaxe). Une requête se compose de
plusieurs parties :

- Premièrement, le point d'accès. À un point d'accès correspond un format, un
  support et des données (défini par une URL, p.ex. `sqlite://:memory:` ou
  `file:///var/local/dyko/`). Techniquement, deux points d'accès différents
  peuvent accéder aux même données, mais de façon différente. On pourra, par
  exemple, accéder à un dossier contenant des vidéos en les traitant comme de
  atomes (un fichier vidéo), des capsules (composées d'images) ou du texte brut
  (sait-on jamais).
- Deuxièmement, la suite de propriétés sur lesquelles on applique un filtre. On
  veut par exemple les items dont la propriété `name` est « toto » et
  l'attribut `parent_directory` est « tata ». Nous parlerons plus loin des
  syntaxes raccourcies.

On voit facilement qu'une requête peut, selon les cas, renvoyer zéro, un ou
plusieurs items. On peut cependant s'assurer qu'une requête renvoie un et un
seul élément ou génère une exception via la fonction apropriée.

----------------------------------------
Glossaire : (à décrire plus précisément)
----------------------------------------

Kalamar
  C'est bien et ça rox !

Atome
  Bloc indivisible de données.
  
Capsule
  Liste ordonnées de capsules et d'atomes.
  
Item
  Capsule ou atome. Un item possède des propriétés sous la forme clé->valeur.
  Par défaut, les propriétés d'un objet ont la valeur `None`. Un lecture d'une
  propriété non instancié renvoie `None`.

Support
  Moyen physique de stockage des données. Ce peut être un SGBD, un système de
  fichiers, un annuaire LDAP etc… Un support stocke et organise des items. À
  chaque Support correspond un accesseur (cf. définition).

Format
  Organisation physique des données d'un item. Un format peut être atomique ou
  non, il est alors une capsule. À chaque format correspond un extracteur
  (cf. définition). Attention à ne pas confondre la notion de format au sens de
  kalamar avec la notion de format de fichier au sens usuel.

Accesseur
  Classe python permettant d'utiliser un support. Techniquement, elle offre une
  interface standard d'accès à un support (lequel a, en principe, son API
  propre). Un accesseur peut définir plusieurs propriétés pour chaque item.

Extracteur
  Classe python permettant d'extraire les propriétés d'un item à partir des
  données brutes stockées sur le support.

Atome
  TODO
  
Capsule
  TODO
  
Item
  TODO
  
Requête
  TODO

Point d'accès (= « point d'entrée »)
  TODO

====================
Description de l'API
====================

Kalamar se présente sous forme d'un module python à importer (non, on ne peut
pas consommer sur place. Ok je sors). Ce module contient une classe
`KalamarSite` dont on créera une instance par site, permettant ainsi de faire
servir plusieurs sites par un même processus.

Schéma ::

  Kalamar
   \_ KalamarSite
   | \_ __init__(String: path_to_config)
   | |_ open(?String: request) -> returns one item or raise an exception if many
   | |_ search(?String: request) -> returns a list of items (0..*)
   | |_ delete(Item: to_delete) -> returns nothing
   | |_ save(Item: to_save) -> returns nothing
   | |_ url_to_request(String: url) -> returns a request giving access to the object
   |
   |_ Item
   | \_ get_property(String: name) -> python object or None if the attirubute doesn't exist
   | |_ set_property(String: name, object : value) -> nothing
   | |_ has_property(String: name)
   | |_ prop: properties (a property called properties that is a defaultdic)
   |
   |_ Atom(Item)
   | \_ read()
   | |_ write(object
   |
   |_ Capsule(Item)
   | \_ list()
   | |_ add_item(Item : )

=======================================
Description du fichier de configuration
=======================================

--------------
TODO TODO TODO
--------------

