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

from mo_streams import ByteStream
from mo_streams.tuple_stream import TupleStream


class ObjectStream:
    def __init__(self, values, example, datatype):
        self._iter: Iterator[Any] = values
        self._example = example
        self._type: Type = datatype

    def __getattr__(self, item):
        if hasattr(ObjectStream, item):
            Log.error("ambigious")

        accessor = getattr(self._example, item)
        method = getattr(self._type, item)
        if type(method).__name__ in {"method_descriptor", "function"}:
            return MethodStream(
                (getattr(v, item) for v in self._iter), accessor, type(accessor)
            )
        else:
            return ObjectStream(
                (getattr(v, item) for v in self._iter), accessor, type(accessor)
            )

    def map(self, accessor):
        if isinstance(accessor, str):
            example = getattr(self._example, accessor)
            return ObjectStream((getattr(v, accessor) for v in self._iter), example, type(example))
        if accessor in self._type:
            example = accessor(self._example)
            return ObjectStream((accessor(v) for v in self.__iter), example, type(example))

    def exists(self):
        example = None
        while example == None:
            try:
                example = next(self._iter)
            except StopIteration:
                return EmptyStream()

        def read():
            yield example
            for v in self._iter:
                if v != None:
                    yield v

        return ObjectStream(read(), example, type(example))

    def to_list(self):
        return list(self._iter)

    def enumerate(self):
        return TupleStream(((v, i) for i, v in enumerate(self._iter)), Tuple[int, self._type])




class MethodStream(ObjectStream):
    def __call__(self, *args, **kwargs):
        example = self._example(*args, **kwargs)
        if type(example) is bytes:
            return ByteStream((m(*args, **kwargs) for m in self._iter))
        return ObjectStream(
            (m(*args, **kwargs) for m in self._iter), example, type(example)
        )

