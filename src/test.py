#!/usr/bin/python

import kalamar
from pprint import pprint
s = kalamar.Site('../test_data/kalamar.conf')
l = list(s.search('toto', 'formation=bpco/chapitre=basesbpco/diaporama/nome=assistancerespiratoire'))
print l[0].read()
