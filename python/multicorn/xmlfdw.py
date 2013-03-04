"""
An XML Foreign Data Wrapper.
"""

from . import ForeignDataWrapper
from xml.sax import ContentHandler, make_parser

class MulticornXMLHandler(ContentHandler):

    def __init__(self, elem_tag, columns):
        self.elem_tag = elem_tag
        self.columns = columns
        self.reset()

    def reset(self):
        self.parsed_rows = []
        self.current_row = {}
        self.tag = None
        self.root_seen = 0
        self.nested = False

    def startElement(self, name, attrs):
        if name == self.elem_tag:
            # Keep track of nested "elem_tag"
            self.root_seen += 1
        elif self.root_seen == 1:
            # Ignore nested tag.
            if name in self.columns:
                self.tag = name
                self.current_row[name] = ''

    def characters(self, content):
        if self.tag is not None:
            self.current_row[self.tag] += content

    def get_rows(self):
        """Return the parsed_rows, and forget about it."""
        result, self.parsed_rows = self.parsed_rows, []
        return result

    def endElement(self, name):
        if name == self.elem_tag:
            self.root_seen -= 1
            self.parsed_rows.append(self.current_row)
            self.current_row = {}
        elif name in self.columns:
            self.tag = None

class XMLFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing xml files.

      Valid options:
        - filename: full path to the xml file.
        - elem_tag: a tagname acting as a root for a tag.
               Child tag will be mapped to corresponding columns.
    """

    def __init__(self, fdw_options, fdw_columns):
        super(XMLFdw, self).__init__(fdw_options, fdw_columns)
        self.filename = fdw_options['filename']
        self.elem_tag = fdw_options['elem_tag']
        self.buffer_size = fdw_options.get('buffer_size', 4096)
        self.columns = fdw_columns

    def execute(self, quals, columns):
        parser = make_parser()
        handler = MulticornXMLHandler(self.elem_tag, self.columns)
        parser.setContentHandler(handler)
        with open(self.filename) as stream:
            while(True):
                a = stream.read(self.buffer_size)
                if not a:
                    break
                parser.feed(a)
                for row in handler.get_rows():
                    yield row
        parser.close()
