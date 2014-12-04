# -*- coding: utf-8 -*-
from docutils.parsers.rst import Directive
from docutils.parsers.rst import directives
from docutils.nodes import Element
from sphinx.builders.html import StandaloneHTMLBuilder


class api_compat(Element):

    def __init__(self, api=None):
        self.api = api or {}
        super(api_compat, self).__init__()

def visit_api_compat_node_html(self, node):
    self.body.append(u'<span class="api_compatibility">Supports: %s' % "".join(
        [u'<i class="compat-%s %s" title="%s">%s</i>' %
         (key, "checked" if val else "unchecked", key,
          u"✓"  if val else u"✗")
         for (key, val) in node.api.items()]))


def depart_api_compat_node_html(self, node):
    self.body.append("</span>")


def visit_api_compat_node_text(self, node):
    self.add_text("Supported API: %s" % ",".join(node.api))


def depart_api_compat_node_text(self, node):
    pass


def visit_api_compat_node_latex(self, node):
    # TODO: make it render in latex
    classes = node.get('classes', [])
    self.body.append(r'\DUspan{%s}{' % ','.join(classes))


def depart_api_compat_node_latex(self, node):
    pass


def setup(app):
    app.add_directive('api_compat', APICompatDirective)
    app.add_node(api_compat,
                 html=(visit_api_compat_node_html, depart_api_compat_node_html),
                 latex=(visit_api_compat_node_latex, depart_api_compat_node_latex),
                 text=(visit_api_compat_node_text, depart_api_compat_node_text))


class APICompatDirective(Directive):

    has_content = True

    option_spec = {
        'read': directives.flag,
        'write': directives.flag,
        'transaction': directives.flag,
#        'import_schema': directives.flag
    }

    def run(self):
        values = {key: key in self.options
                  for key in self.option_spec}
        return [api_compat(api=values)]
