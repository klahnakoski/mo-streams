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
            for tup in self._iter:
                try:
                    yield (getattr(tup[0], item),) + tup[1:]
                except Exception:
                    yield (None,) + tup[1:]

        return TupleStream(read(), accessor, type(accessor))

    def __call__(self, *args, **kwargs):
        example = self._example(*args, **kwargs)

        def read():
            for tup in self._iter:
                try:
                    yield (tup[0](*args, **kwargs),) + tup[1:]
                except Exception:
                    yield (None,) + tup[1:]

        if isinstance(example, bytes):
            return ByteStream(Reader(read()))

        return TupleStream(read(), example, type(example))

    def map(self, accessor):
        def read():
            for tup in self._iter:
                try:
                    new_value = accessor(tup[0])
                    yield (new_value,) + tup[1:]
                except Exception:
                    yield (None,) + tup[1:]

        example = accessor(self._example)
        return TupleStream(read(), example, type(example))

    def exists(self):
        tup = (None,)
        while tup[0] == None:
            try:
                tup = next(self._iter)
            except StopIteration:
                return EmptyStream()

        def read(tup):
            yield tup
            for tup in self._iter:
                if tup[0] != None:
                    yield tup

        return TupleStream(read(*tup), tup[0], type(tup[0]))

    def to_dict(self):
        return {tup[1]: tup[0] for tup in self._iter}
