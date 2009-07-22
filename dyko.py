#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

import kraken
from werkzeug.serving import run_simple

def runserver(site_root=('s', '.'), kalamar_conf=('k', ''),
              hostname=('h', 'localhost'), port=('p', 5000),
              reloader=('r', False), debugger=('d', False),
              evalex=('e', True), threaded=('t', False), processes=1):
    """Start a Dyko server instance.
    
    If --kalamar_conf is not given, a default test server will be run with
    --reloader and --debugger options.
    If --kalamar_conf is given, a basic python server will be run
    using this configuration.
    
    """
    if kalamar_conf:
        site = kraken.Site(site_root, kalamar_conf)
    else:
        site = kraken.Site(site_root)
    run_simple(hostname=hostname, port=port, application=site,
                use_reloader=reloader, use_debugger=debugger,
                use_evalex=evalex, processes=processes, threaded=threaded)

def main(*args):
    werkzeug.script.run(namespace={'runserver': runserver}, action_prefix='',
                        args=['runserver'] + list(args))

if __name__ == '__main__':
    main(*sys.argv[1:])
