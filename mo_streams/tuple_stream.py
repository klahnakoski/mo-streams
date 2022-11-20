# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Any, Tuple, Type, Iterator

from mo_dots.lists import Log

from mo_streams import ByteStream, EmptyStream
from mo_streams.object_stream import ObjectStream
from mo_streams.utils import Reader


class TupleStream:
    """
    A STREAM OF OBJECTS

    ASSUME TRANSFORMATIONS ARE DONE ON value, LEAVING THE REST UNCHANGED TO TAG ALONG
    """
    def __init__(self, tuples, example, datatype):
        self._iter: Iterator[Tuple[Any]] = tuples
        self._example = example
        self._type: Type = datatype

    def __getattr__(self, item):
        if hasattr(ObjectStream, item):
            Log.error("ambigious")

        accessor = getattr(self._example, item)

        def read():
            for v, *r in self._iter:
                try:
                    yield getattr(v, item), *r
                except Exception:
                    yield None, *r

        return TupleStream(read(), accessor, type(accessor))

    def __call__(self, *args, **kwargs):
        example = self._example(*args, **kwargs)

        def read():
            for m, *r in self._iter:
                try:
                    yield m(*args, **kwargs), *r
                except Exception:
                    yield None, *r

        if isinstance(example, bytes):
            return ByteStream(Reader(read()))

        return TupleStream(read(), example, type(example))

    def map(self, accessor):
        def read():
            for v, *r in self._iter:
                try:
                    new_value = accessor(v)
                    yield new_value, *r
                except Exception:
                    yield None, *r

        example = accessor(self._example)
        return TupleStream(read(), example, type(example))

    def exists(self):
        first_value = None
        rest = None
        while first_value == None:
            try:
                first_value, *rest = next(self._iter)
            except StopIteration:
                return EmptyStream()

        def read(value, *r):
            yield value, *r
            for v, *r in self._iter:
                if v != None:
                    yield v, *r

        return TupleStream(read(first_value, *rest), first_value, type(first_value))

    def to_dict(self):
        return {k: v for v, k, *r in self._iter}
