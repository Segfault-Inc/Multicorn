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
  version…).
- De plusieurs modules (« extracteurs ») propres à chaque format de données et
  permettant d'extraire des propriétés (p.ex. : image, MPEG, texte, ReST,
  HTML, RAW…). À un même « format » au sens usuel, on peut attribuer plusieurs
  « formats » différents au sens Kalamar, c'est à dire différents extracteurs
  (un document XML peut être considéré comme un document texte par exemple, ou
  comme une image si c'est du SVG).

Les formats donnés sont divisés conceptuellement en deux catégories : les
« atomes » et les « capsules ». Atomes et capsules sont tous des « items ».
Tous les items ont une liste de « propriétés » générées par leur extracteur et
leur accesseur.

- Les atomes sont des éléments indivisibles d'information. Ils ne possèdent que
  des propriétés et un contenu (qui est en fait une propriété particulière).
- Les capsules sont des listes ordonnées d'items (atomes ou capsules). Elles possèdent des
  propriétés mais pas de contenu (on peut tricher et leur en faire contenir si
  on veut programmer salement, c'est à vos risques et périls…).

Ainsi, un atome image et une capsule image ne sont pas le même format (et
n'auront pas le même nom, p.ex. ImageAtom et ImageCapsule). En effet, l'atome
donne accès aux données brutes de l'image (et vous vous débrouillez avec) alors
que la capsule peut donner accès à chaque pixel.

Les items sont accessibles via des requêtes sous forme de chaîne de caractères.
Une requête se compose de plusieurs parties :

- Premièrement, le point d'accès. À un point d'accès correspond un format, un
  support et des données (définies par une URL, p.ex. `sqlite://:memory:` ou
  `file:///var/local/dyko/`). Techniquement, deux points d'accès différents
  peuvent accéder aux même données, mais de façon différente. On pourra, par
  exemple, accéder à un dossier contenant des vidéos en les traitant comme de
  atomes (un fichier vidéo), des capsules (composées d'images) ou du texte brut
  (sait-on jamais).
- Deuxièmement, la suite de propriétés sur lesquelles on applique un filtre. On
  veut par exemple les items dont la propriété `name` est « toto » et
  l'attribut `parent_directory` est « tata ».

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
  chaque support correspond un accesseur (cf. définition).

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

Propriété
  Couple (nom,objet) où « nom » est une chaine de caractères unicode et
  « objet » est un objet python.Les propriétés définies par l'accesseur sont
  prioritaires sur celle définies par l'extracteur.
  
Requête
  Chaîne de caractères donnée au moteur kalamar permettant de séléctionner des
  items parmis ceux accessibles dans un point d'accès. Une requête doit suivre
  la syntaxe suivante ::
  
    contraintes :
      filtre sur les propriétés des items, indépendamment du support et du format
    opérateurs de comparaison: = < > <= >= ~= ! et leur négation
    opérateurs logiques : "/" est le "et", "|" est le "ou"
    priorité de gauche à droite.
    Le "!" et prioritaire sur tous les autres.
    Sucre syntaxique : on peut écrire une requête sous la forme "muse/showbiz/3".
    C'est alors équivalent à "artist=muse/album=showbiz/track=3" où artist, album
    et track sont définits dans la section "aliases" de la configuration.
    

Point d'accès
  Nom [a-zA-Z0-9[_-]] désignant un ensemble d'items ayant même support, même
  format et même url de base. De plus, il a une option spécifiant l'encodage de
  caractère par défaut.

====================
Description de l'API
====================

Kalamar se présente sous forme d'un module python à importer (non, on ne peut
pas consommer sur place. Ok je sors). Ce module contient une classe
`KalamarSite` dont on créera une instance par site, permettant ainsi de faire
servir plusieurs sites par un même processus.

Schéma ::

  Kalamar
   \_ Site
   | \_ __init__(path_to_config)
   | |_ open(request) -> returns one item or raise an exception if many.
   | |    Actually, it will never return an instance of Item but one of a
   | |    descendant of Atom or Capsule.
   | |_ search(request) -> returns a list of items (0..*)
   | |_ delete(anItem) -> returns nothing
   | |_ save(anItem) -> returns nothing
   | |_ url_to_request(url) -> returns a request giving access to the object
   |
   |_ <abstract> BaseItem
   | \_ prop: properties (a property called properties that looks like a defaultdic)
   | |_ access_point
   | |_ _get_encoding() -> return the item's encoding, based on what the extractor
   | |    can know from the items's data or, if unable to do so, on what is specified
   | |    in the access_point.
   | |_ matches(propertie, operator, value) -> return boolean
   |
   |_ <abstract> AtomItem(Item)
   | \_ read()
   | |_ write(object)
   |
   |_ <abstract> CapsuleItem(Item)
   | \_ list()
   | |_ add_item(anItem)
   |
   |_ AccessPoint
   | \_ __init__(name, accessor, extractor, encoding="utf8")

=======================================
Description du fichier de configuration
=======================================

Il y a un fichier de configuration par site. Ce fichier contient une section par
point d'accès. Pour chacun de ces point d'accès, on liste ses paramètres.

Exemple (fichier `/etc/kalamar/MonSite.cfg`) ::

  [an_access_point]
  # alias=original/alias2=original2 ...
  accessor_aliases : genre=path1/auteur=path2/album=path3/titre=path4
  parser_aliases : disque=diskno/no=bloblo
  # The * will be mapped to path1, path2 ...
  filename_format : */*/* - *.mp3
  url : file:///var/local/music/
  parser : a_format_name
  
  [another_access_point]
  accessor_aliases : no=track/auteur=artist
  parser_aliases : album=album/blabla=bloblo
  url : mysql://user:passwd@host:port/base/table
  parser : another_format_name

