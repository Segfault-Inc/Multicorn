# coding: utf8
"""
A quick and dirty script to build the ReST test data
"""

import os.path
base = os.path.dirname(__file__)

for dir in ('fs_rest_messed_up', 'fs_rest_capsules'):
    for name in os.listdir(os.path.join(base, dir)):
        os.remove(os.path.join(base, dir, name))

albums = {}

for name in os.listdir(os.path.join(base, 'fs_text_messed_up')):
    new_name = os.path.splitext(name)[0] + '.rst'
    text = open(os.path.join(base, 'fs_text_messed_up', name))
    rest = open(os.path.join(base, 'fs_rest_messed_up', new_name), 'w')
    
    props = dict(zip(
        ('genre', 'artist', 'album', 'tracknumber', 'title'),
        (value for value in text.read().split('\n'))
    ))
    
    key = (props['genre'], props['artist'], props['album'])
    value = (props['tracknumber'], props['title'], name)
    albums.setdefault(key, []).append(value)
    
    title = props.pop('title')
    
    write = lambda s: rest.write(s + '\n')
    write('=' * len(title))
    write(title)
    write('=' * len(title))
    for item in props.items():
        write(':%s: %s' % item)

for (genre, artist, album), tracks in albums.items():
    name = '%s - %s - %s.rst' % (genre, artist, album)
    rest = open(os.path.join(base, 'fs_rest_capsules', name), 'w')
    write = lambda s: rest.write(s + '\n')
    write('=' * len(album))
    write(album)
    write('=' * len(album))
    write(':%s: %s' % ('genre', genre))
    write(':%s: %s' % ('artist', artist))
    for tracknumber, title, filename in sorted(tracks):
        write('.. include:: %s' % os.path.join(os.path.pardir, # '..'
                                               'fs_text_messed_up', filename))


