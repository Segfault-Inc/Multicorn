# coding: utf8
"""
A quick and dirty script to build the ReST test data
"""

import os.path
base = os.path.dirname(__file__)

for name in os.listdir(os.path.join(base, 'fs_rest_messed_up')):
    os.remove(os.path.join(base, 'fs_rest_messed_up', name))

for name in os.listdir(os.path.join(base, 'fs_text_messed_up')):
    text = open(os.path.join(base, 'fs_text_messed_up', name))
    rest = open(os.path.join(base, 'fs_rest_messed_up', name), 'w')
    
    props = dict(zip(
        ('genre', 'artist', 'album', 'tracknumber', 'title'),
        (value for value in text.read().split('\n'))
    ))
    title = props.pop('title')
    
    write = lambda s: rest.write(s + '\n')
    write('=' * len(title))
    write(title)
    write('=' * len(title))
    for item in props.items():
        write(':%s: %s' % item)
    


