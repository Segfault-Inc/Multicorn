#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

import kraken
from werkzeug.serving import run_simple

def action_runserver(kalamar_conf=('k', ''), root=('r', '.'),
                     hostname=('h', 'localhost'), port=('p', 5000),
                     reloader=('l', False), debugger=('d', False),
                     evalex=('e', True), threaded=('t', False), processes=1):
    """Start a Dyko server instance."""
    site = kraken.Site(root, kalamar_conf)
    run_simple(hostname=hostname, port=port, application=site,
                use_reloader=reloader, use_debugger=debugger,
                use_evalex=evalex, processes=processes, threaded=threaded)

def main(*args):
    werkzeug.script.run(namespace=dict(action_runserver=action_runserver),
                        args=list(args))

if __name__ == '__main__':
    main(*sys.argv[1:])
