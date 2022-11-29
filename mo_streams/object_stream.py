# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Any, Iterator, Dict, Tuple
from zipfile import ZIP_STORED

from mo_files import File
from mo_future import zip_longest
from mo_imports import delay_import, expect
from mo_logs import logger

from mo_json import JxType, JX_INTEGER
from mo_streams import ByteStream
from mo_streams.function_factory import factory
from mo_streams.type_utils import Typer, LazyTyper
from mo_streams.utils import Reader, Writer, os_path, chunk_bytes, Stream, File_usingStream

TupleStream = delay_import("mo_streams.tuple_stream.TupleStream")
stream = expect("stream")


class ObjectStream(Stream):
    """
    A STREAM OF OBJECTS
    """

    def __init__(self, values, datatype, schema):
        if datatype is None or isinstance(datatype, LazyTyper):
            logger.error("expecting datatype to not be none")
        self._iter: Iterator[Tuple[Any, Dict[str, Any]]] = values
        self.type_: Typer = datatype
        self._schema = schema

    def __getattr__(self, item):
        type_ = getattr(self.type_, item)

        def read():
            for v, a in self._iter:
                try:
                    yield getattr(v, item), a
                except Exception:
                    yield None

        return ObjectStream(read(), type_, self._schema)

    def __call__(self, *args, **kwargs):
        type_ = self.type_(*args, **kwargs)

        def read():
            for m, a in self._iter:
                try:
                    yield m(*args, **kwargs), a
                except Exception:
                    yield None

        if type_ == bytes:
            return ByteStream(Reader(read()), self._schema)

        return ObjectStream(read(), type_, self._schema)

    def map(self, accessor):
        if isinstance(accessor, str):
            type_ = getattr(self.type_, accessor)
            return ObjectStream(
                ((getattr(v, accessor), a) for v, a in self._iter), type_, self._schema
            )
        fact = factory(accessor, self.type_)
        do_accessor = fact.build(self.type_, self._schema)

        def read():
            for v, a in self._iter:
                try:
                    yield do_accessor(v, a), a
                except Exception:
                    yield None

        if isinstance(fact.type_, LazyTyper):
            type_ = fact.type_._resolver(self.type_)
        else:
            type_ = fact.type_

        return ObjectStream(read(), type_, self._schema)

    def attach(self, **kwargs):

        facts = {k: factory(v) for k, v in kwargs.items()}

        more_schema = JxType()  # NOT AT REAL TYPE, WE ADD PYTHON TYPES ON THE LEAVES
        for k, f in facts.items():
            setattr(more_schema, k, f.type_)

        mapper = {k: f.build(f.type_, self._schema) for k, f in facts.items()}

        def read():
            for v, a in self._iter:
                yield v, {**a, **{k: m(v, a) for k, m in mapper.items()}}

        return ObjectStream(read(), self.type_, self._schema | more_schema)

    def exists(self):
        def read():
            for v, a in self._iter:
                if v != None:
                    yield v, a

        return ObjectStream(read(), self.type_, self._schema)

    def enumerate(self):
        def read():
            for i, (v, a) in enumerate(self._iter):
                yield v, {**a, "index": i}
        return ObjectStream(read(), self.type_, self._schema+JxType(index=JX_INTEGER))

    def flatten(self):
        def read():
            for v, a in self._iter:
                for vv, aa in stream(v)._iter:
                    yield vv, {**a, **aa}

        return ObjectStream(read(), self.type_, self._schema)

    def reverse(self):
        def read():
            yield from reversed(list(self._iter))

        return ObjectStream(read(), self.type_, schema=self._schema)

    def sort(self, *, key=None, reverse=0):
        def read():
            yield from sorted(self._iter, key=lambda t: key(t[0]), reverse=reverse)

        return ObjectStream(read(), self.type_, self._schema)

    def distinct(self):
        def read():
            acc = set()
            for v, a in self._iter:
                if v in acc:
                    continue
                acc.add(v)
                yield v, a

        return ObjectStream(read(), self.type_, self._schema)

    def append(self, value):
        def read():
            yield from self._iter
            yield value, {}

        return ObjectStream(read(), self.type_, self._schema)

    def extend(self, values):
        suffix = stream(values)
        def read():
            yield from self._iter
            yield from suffix._iter

        return ObjectStream(read(), self.type_, self._schema+suffix._schema)

    def zip(self, *others):
        streams = [stream(o) for o in others]

        def read():
            yield from zip_longest(self._iter, *(s._iter for s in streams))

        return TupleStream(read(), self._example, self.type_, sum((s._schema for s in streams), JxType()))

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
        return ObjectStream(list(self._iter), self.type_, self._schema)

    def to_list(self):
        return list(v for v, _ in self._iter)

    def to_zip(
        self, compression=ZIP_STORED, allowZip64=True, compresslevel=None,
    ):
        from zipfile import ZipFile, ZipInfo

        if self.type_.type_ not in (File, File_usingStream):
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

        return ByteStream(Reader(read()), self._schema)

