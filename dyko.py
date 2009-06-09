#!/usr/bin/env python

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


if __name__ == '__main__':
    from kraken import script
    script.run()

