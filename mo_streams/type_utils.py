# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import inspect

from mo_logs import logger

from mo_streams.type_parser import parse


class Typer:
    """
    Handle the lumps of python type manipulation
    """
    def __init__(self, *, example=None, type_=None, function=None):
        if function:
            # find function return type
            inspect.signature(function)
        elif example:
            self.type_ = type(example)
        else:
            self.type_ = type_

    def __getattr__(self, item):
        for name, func in inspect.getmembers(self.type_):
            if name != item:
                continue
            desc = inspect.getfullargspec(func)
            return_type = desc.annotations.get('return')
            if not return_type:
                logger.error("expecting {self.type_.type_.__name__}.{item} to have annotated return type")
            return parse(return_type)

    def __add__(self, other):
        if self.type_ is str or other.type_ is str:
            return Typer(type_=str)
        logger.error("not handled")

    def __str__(self):
        return f"Typer(class={self.type_.__name__})"


class LazyTyper(Typer):
    """
    PLACEHOLDER FOR STREAM ELEMENT TYPE, UNKNOWN DURING LAMBDA DEFINTION
    """

    def __init__(self, resolver=None):
        Typer.__init__(self)
        self._resolver : Typer = resolver or (lambda t: t)

    def __getattr__(self, item):
        def build(type_):
            return getattr(type_, item)
        return LazyTyper(build)

    def __str__(self):
        return "LazyTyper()"

