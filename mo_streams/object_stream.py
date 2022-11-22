# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Any, Type, Iterator
from zipfile import ZIP_STORED

from mo_files import File
from mo_imports import delay_import, expect

from mo_streams import ByteStream, EmptyStream
from mo_streams.utils import Reader, Writer, os_path, chunk_bytes

TupleStream = delay_import("mo_streams.tuple_stream.TupleStream")
stream = expect("stream")


class ObjectStream:
    """
    A STREAM OF OBJECTS
    """

    def __init__(self, values, example, datatype):
        self._iter: Iterator[Any] = values
        self._example = example
        self._type: Type = datatype

    def __getattr__(self, item):
        accessor = getattr(self._example, item)

        def read():
            for v in self._iter:
                try:
                    yield getattr(v, item)
                except Exception:
                    yield None

        return ObjectStream(read(), accessor, type(accessor))

    def __call__(self, *args, **kwargs):
        example = self._example(*args, **kwargs)

        def read():
            for m in self._iter:
                try:
                    yield m(*args, **kwargs)
                except Exception:
                    yield None

        if isinstance(example, bytes):
            return ByteStream(Reader(read()))

        return ObjectStream(read(), example, type(example))

    def map(self, accessor):
        if isinstance(accessor, str):
            example = getattr(self._example, accessor)
            return ObjectStream(
                (getattr(v, accessor) for v in self._iter), example, type(example)
            )

        example = accessor(self._example)

        def read():
            for v in self._iter:
                try:
                    yield accessor(v)
                except Exception:
                    yield None
        return ObjectStream(read(), example, type(example))

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

    def enumerate(self):
        return TupleStream(
            ((v, i) for i, v in enumerate(self._iter)), self._example, self._type
        )

    def reverse(self):
        def read():
            yield from reversed(list(self._iter))

        return ObjectStream(read(), self._example, self._type)

    def sort(self, *, key=None, reverse=0):
        def read():
            yield from sorted(self._iter, key=key, reverse=reverse)

        return ObjectStream(read(), self._example, self._type)

    def distinct(self):
        def read():
            acc = set()
            for v in self._iter:
                if v in acc:
                    continue
                acc.add(v)
                yield v

        return ObjectStream(read(), self._example, self._type)

    def append(self, value):
        def read():
            yield from self._iter
            yield value

        return ObjectStream(read(), self._example, self._type)

    def extend(self, values):
        def read():
            yield from self._iter
            yield stream(values)

        return ObjectStream(read(), self._example, self._type)

    def materialize(self):
        pass

    def to_list(self):
        return list(self._iter)

    def to_zip(
        self, compression=ZIP_STORED, allowZip64=True, compresslevel=None,
    ):
        from zipfile import ZipFile, ZipInfo

        if not isinstance(self._example, File):
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
                for file in self._iter:
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


class MethodStream(ObjectStream):
    def __call__(self, *args, **kwargs):
        example = self._example(*args, **kwargs)
        if type(example) is bytes:
            return ByteStream((m(*args, **kwargs) for m in self._iter))
        return ObjectStream(
            (m(*args, **kwargs) for m in self._iter), example, type(example)
        )
