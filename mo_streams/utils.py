# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from io import BytesIO, BufferedReader
from typing import Tuple, BinaryIO

from mo_dots.lists import Log
from mo_files import File
from mo_future import get_function_name
from mo_imports import delay_import

ByteStream = delay_import("mo_streams.byte_stream.ByteStream")
TupleStream = delay_import("mo_streams.tuple_stream.TupleStream")


class Reader(BinaryIO):
    def __init__(self, chunks):
        self._chunks = chunks
        self.residue = b""
        self.count = 0

    def read(self, size):
        if not self._chunks:
            return self._more(size)

        try:
            if size == -1:
                data = next(self._chunks)
                self.count += len(data)
                return data

            while len(self.residue) < size:
                self.residue += next(self._chunks)
        except StopIteration:
            self._chunks = None
        return self._more(size)

    def _more(self, size):
        data = self.residue[:size]
        self.residue = self.residue[size:]
        self.count += len(data)
        return data

    def tell(self):
        return self.count

    def seek(self, position):
        if self.count > position:
            raise NotImplementedError()
        self.read(position-self.count)



def chunk_bytes(stream, size=4096):
    def read():
        try:
            while True:
                data = stream.read(size)
                if not data:
                    return
                yield data
        except Exception as e:
            Log.error("Problem iterating through stream", cause=e)
        finally:
            try:
                stream.close()
            except Exception as cause:
                pass

    return read()


def extend(cls):
    """
    DECORATOR TO ADD METHODS TO CLASSES
    :param cls: THE CLASS TO ADD THE METHOD TO
    :return:
    """

    def extender(func):
        setattr(cls, get_function_name(func), func)
        return func

    return extender


class StreamFile:

    def __init__(self, name, content):
        self.name = name
        self._content = content

    def content(self):
        return self._content()


@extend(File)
def content(self):
    if self.extension == "zst":
        import zstandard

        stream_reader = (
            zstandard
            .ZstdDecompressor(max_window_size=2147483648)
            .stream_reader(Reader(chunk_bytes(BufferedReader(open(self.abspath, "rb")))))
        )
        return ByteStream(chunk_bytes(stream_reader))
    if self.extension == "zip":
        import zipfile

        archive = zipfile.ZipFile(open(self.abspath, "rb"), mode="r")
        names = archive.namelist()
        return TupleStream(
            ((ByteStream(chunk_bytes(BufferedReader(archive.open(name, "r")))), name) for name in names),
            (ByteStream(BytesIO(b"")), ""),
            Tuple[ByteStream, str],
        )

    return ByteStream(chunk_bytes(BufferedReader(open(self.abspath, "rb"))))
