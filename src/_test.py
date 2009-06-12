#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This file is part of Koral - XML generation library
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
Kalamar testing script.

This script launches tests in all files in the active folder and all its
subfolders (except testing files).
"""

# TODO: Freeze and comment this file

import os
import sys
import doctest

_module_name = ""

_total_attempted = 0
_total_failed = 0
_total_todo = 0

def _print_title(light="", bold="", color=35, underline=True, overline=False):
    if light and bold:
        light += " "

    length = len(light + bold)
    lines = []

    lines.append("\033[0;%(color)im"%{
        "color": color,
        })

    if overline:
        lines[-1] += length * "="
        lines.append("")

    lines[-1] += "%(light)s\033[1;%(color)im%(bold)s\033[0;%(color)im"%{
        "light": light,
        "bold": bold,
        "color": color,
        }

    if underline:
        lines.append(length * "=")
        lines.append("")

    lines[-1] += "\033[m"

    print "\n".join(lines)

def _print_path(path):
    _print_title("Entering", "%(folder)s"%{
            "folder": os.path.relpath(path, os.pardir),
            })

def _print_results(name, attempted, failed, todo):
    if name:
        _print_title("Testing", name, color=34, underline=False)
        
    print("""\
\033[%(a)sm %(attempted)i attempted \033[m
\033[%(f)sm %(failed)i failed \033[m
\033[%(t)sm %(todo)i TODO \033[m
"""%{

            "attempted": attempted,
            "failed": failed,
            "todo": todo,
            
            "a": "1;31" if not attempted else "0;32",
            "f": "1;31" if failed else "0;32",
            "t": "1;31" if todo else "0;32",
            })    

def _test(module, path, name):
    global _total_attempted, _total_failed, _total_todo

    failed, attempted = doctest.testmod(module, report=False)
    todo = open(os.path.join(path, "%s.py"%name)).read().count("TODO")

    _total_attempted += attempted
    _total_failed += failed
    _total_todo += todo

    _print_results(name, attempted, failed, todo)

def _test_folder(_, path, names):
    if path[0] != "." and os.path.basename(path) != "_test":
        names = [os.path.splitext(name)[0]
                 for name in names
                 if os.path.splitext(name)[0] != "_test"
                 and os.path.splitext(name)[1] == ".py"]

        folder = os.path.relpath(path)
        folders = [fold for fold in folder.split(os.sep) if fold != "."]

        if names:
            _print_path(path)
            names.sort()

        for name in names:
            module = ".".join([_module_name] + folders + [name])
            __import__(module)
            _test(sys.modules[module], folder, name)

def test_module():
    _print_title("Testing Module", _module_name, overline=True)
    os.path.walk(os.getcwd(), _test_folder, None)
    _print_title(bold="Total Results", overline=True)
    _print_results("", _total_attempted, _total_failed, _total_todo)

if __name__ == "__main__":
    _module_name = os.path.basename(os.getcwd())

    sys.path.remove(os.getcwd())
    sys.path.append(os.path.dirname(os.getcwd()))

    test_module()
