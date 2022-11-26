# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Any, Type, Iterator, Dict, Tuple
from zipfile import ZIP_STORED

import boto3
from mo_files import File
from mo_future import zip_longest, first
from mo_imports import delay_import, expect
from mo_json import JxType, python_type_to_jx_type, JX_INTEGER, JX_TEXT

from mo_streams import ByteStream, EmptyStream
from mo_streams.function_factory import FunctionFactory, factory
from mo_streams.utils import Reader, Writer, os_path, chunk_bytes, Stream, is_function

TupleStream = delay_import("mo_streams.tuple_stream.TupleStream")
stream = expect("stream")


class ObjectStream(Stream):
    """
    A STREAM OF OBJECTS
    """

    def __init__(self, values, datatype, schema):
        self._iter: Iterator[Tuple[Any, Dict[str, Any]]] = values
        self._type: Type = datatype
        self._schema = schema

    def __getattr__(self, item):
        _type = getattr(self._type, item)

        def read():
            for v, a in self._iter:
                try:
                    yield getattr(v, item), a
                except Exception:
                    yield None

        return ObjectStream(read(), _type, self._schema)

    def __call__(self, *args, **kwargs):
        _type = self._type(*args, **kwargs)

        def read():
            for m, a in self._iter:
                try:
                    yield m(*args, **kwargs), a
                except Exception:
                    yield None

        if _type == bytes:
            return ByteStream(Reader(read()), self._schema)

        return ObjectStream(read(), _type, self._schema)

    def map(self, accessor):
        if isinstance(accessor, str):
            _type = getattr(self._type, accessor)
            return ObjectStream(
                ((getattr(v, accessor), a) for v, a in self._iter), _type, self._schema
            )
        fact = factory(accessor)
        do_accessor = fact.build(self._type, self._schema)

        def read():
            for v, a in self._iter:
                try:
                    yield do_accessor(v, a), a
                except Exception:
                    yield None

        return ObjectStream(read(), fact._type, self._schema)

    def attach(self, **kwargs):

        facts = {k: factory(v) for k, v in kwargs.items()}

        more_schema = JxType()  # NOT AT REAL TYPE, WE ADD PYTHON TYPES ON THE LEAVES
        for k, f in facts.items():
            setattr(more_schema, k, f._type)

        mapper = {k: f.build(self._type, self._schema) for k, f in facts.items()}

        def read():
            for v, a in self._iter:
                yield v, {**a, **{k: m(v) for k, m in mapper.items()}}

        return ObjectStream(read(), self._type, self._schema | more_schema)

    def exists(self):
        def read():
            for v, a in self._iter:
                if v != None:
                    yield v, a

        return ObjectStream(read(), self._type, self._schema)

    def enumerate(self):
        def read():
            for i, (v, a) in enumerate(self._iter):
                yield v, {**a, "index": i}
        return ObjectStream(read(), self._type, self._schema+JxType(index=JX_INTEGER))

    def flatten(self):
        def read():
            for v, a in self._iter:
                for vv, aa in stream(v)._iter:
                    yield vv, {**a, **aa}

        return ObjectStream(read(), self._type, self._schema)

    def reverse(self):
        def read():
            yield from reversed(list(self._iter))

        return ObjectStream(read(), self._type, schema=self._schema)

    def sort(self, *, key=None, reverse=0):
        def read():
            yield from sorted(self._iter, key=lambda t: key(t[0]), reverse=reverse)

        return ObjectStream(read(), self._type, self._schema)

    def distinct(self):
        def read():
            acc = set()
            for v, a in self._iter:
                if v in acc:
                    continue
                acc.add(v)
                yield v, a

        return ObjectStream(read(), self._type, self._schema)

    def append(self, value):
        def read():
            yield from self._iter
            yield value, {}

        return ObjectStream(read(), self._type, self._schema)

    def extend(self, values):
        suffix = stream(values)
        def read():
            yield from self._iter
            yield from suffix._iter

        return ObjectStream(read(), self._type, self._schema+suffix._schema)

    def zip(self, *others):
        streams = [stream(o) for o in others]

        def read():
            yield from zip_longest(self._iter, *(s._iter for s in streams))

        return TupleStream(read(), self._example, self._type, sum((s._schema for s in streams), JxType()))

    def limit(self, count):
        def read():
            try:
                for i in range(count):
                    yield next(self._iter)
            except StopIteration:
                return
            for v in self._iter:
                continue

        return ObjectStream(read(), self._iter, self._schema)

    def materialize(self):
        return ObjectStream(list(self._iter), self._type, self._schema)

    def to_list(self):
        return list(v for v, _ in self._iter)

    def to_zip(
        self, compression=ZIP_STORED, allowZip64=True, compresslevel=None,
    ):
        from zipfile import ZipFile, ZipInfo

        if self._type != File:
            raise NotImplementedError("expecting stream of Files")

        def read():
            mode = "w"
            writer = Writer()
            with ZipFile(
                writer,
                mode=mode,
                compression=compression,
                allowZip64=allowZip64,
                compresslevel=compresslevel,
            ) as archive:
                for file, _ in self._iter:
                    filename = os_path(file.abspath)
                    z_info = ZipInfo.from_file(filename, file.filename)
                    with archive.open(z_info, mode=mode) as target:
                        with open(filename, "rb") as source:
                            for chunk in chunk_bytes(source):
                                target.write(chunk)
                                yield writer.read()

            yield writer.read()
            writer.close()

        return ByteStream(Reader(read()))

