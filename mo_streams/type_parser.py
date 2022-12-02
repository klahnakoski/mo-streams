# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_imports import delay_import

from mo_streams.type_utils import CallableTyper

Typer = delay_import("mo_streams.Typer")


def parse(type_desc):
    types = [
        clean for t in type_desc.split("|") for clean in [t.strip()] if clean != "None"
    ]

    if len(types) == 1:
        if types[0] == "str":
            return CallableTyper(type_=str)

    raise NotImplementedError()
