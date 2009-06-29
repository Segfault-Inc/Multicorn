#!/usr/bin/env python

import sys, os

sys.path.insert(0, os.path.dirname(__file__))


if __name__ == '__main__':
    from kraken import script, site
    script.run(site.Site())

