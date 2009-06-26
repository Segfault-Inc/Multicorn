#!/usr/bin/python
# -*- encoding: utf-8 -*-

import kalamar
from pprint import pprint

s = kalamar.Site('../test_data/kalamar.conf')

#l = list(s.search('toto', 'formation=bpco/chapitre=basesbpco/diaporama/nome=assistancerespiratoire'))
#print l[0].read()

#l = list(s.search('fs_vorbis_classified', 'pop/The Divine Comedy/Liberation/titre=Festive road'))
#pprint(l)

#l = s.open('fs_vorbis_classified', 'pop/The Divine Comedy/Liberation/titre=Festive road')
#pprint(l)

#l = list(s.search('fs_vorbis_messed_up', 'The Divine Comedy/Liberation/titre=Festive road'))
#pprint(l)

#l = s.open('fs_vorbis_messed_up', 'titre=Festive road/album=Liberation')
#pprint(l.properties["titre"])

#l = list(s.search('fs_text_classified', u'title~=.*amen$'))
#for it in l:
#    print ', '.join((it.properties["genre"], it.properties["artiste"], \
#                     it.properties["album"], it.properties["titre"]))

l = list(s.search('fs_text_messed_up', u'jazz'))
for it in l:
    print ', '.join((it.properties["genre"], it.properties["artiste"], \
                     it.properties["album"], it.properties["titre"]))
