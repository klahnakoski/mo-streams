# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import os
from io import RawIOBase
from typing import BinaryIO

from mo_dots.lists import Log
from mo_imports import delay_import
from mo_logs import logger

ByteStream = delay_import("mo_streams.byte_stream.ByteStream")
TupleStream = delay_import("mo_streams.tuple_stream.TupleStream")


class Stream:
    pass


class Reader(BinaryIO):
    """
    WRAP A GENERATOR WITH A FILE-LIKE OBJECT
    """

    def __init__(self, chunks):
        self._chunks = chunks
        self.residue = b""
        self.count = 0

    def readable(self):
        return True

    def read(self, size=-1):
        if not self._chunks:
            return self._more(size)

        try:
            if size == -1:
                data = next(self._chunks)
                self.count += len(data)
                return data

            while len(self.residue) < size:
                chunk = next(self._chunks)
                self.residue += chunk
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
        self.read(position - self.count)


class Writer(RawIOBase):
    """
    REPLACE IO SO THAT WE CAN read() THE RESULTING
    """

    def __init__(self):
        self._buffer = b""

    def writable(self):
        return True

    def write(self, b):
        if self.closed:
            raise Exception("stream was closed")
        self._buffer += b
        return len(b)

    def read(self, size=-1):
        if size == -1 or size > len(self._buffer):
            chunk = self._buffer
            self._buffer = b""
            return chunk

        chunk = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return chunk

    def content(self):
        return ByteStream(self)

    def size(self):
        return len(self._buffer)


def chunk_bytes(reader, size=4096):
    """
    WRAP A FILE-LIKE OBJECT TO LOOK LIKE A GENERATOR
    """

    if isinstance(reader, ByteStream):
        reader = reader.reader
    if isinstance(reader, Reader):
        return reader._chunks

    def read():
        """
        :return:
        """
        try:
            while True:
                data = reader.read(size)
                if not data:
                    return
                yield data
        except Exception as e:
            Log.error("Problem iterating through stream", cause=e)
        finally:
            try:
                reader.close()
            except Exception as cause:
                pass

    return read()


class File_usingStream:
    """
    A File USING A BORROW STREAM.  FOR USE IN TAR AND ZIP FILES
    """

    name: str

    def __init__(self, name, content):
        self.name = name
        self._content = content

    def content(self) -> ByteStream:
        return self._content()


def os_path(path):
    """
    :return: OS-specific path
    """
    if path == None:
        return None
    if os.sep == "/":
        return str(path)
    return str(path).lstrip("/")


def is_function(value):
    if type(value).__name__ == "function":
        return True
    if isinstance(value, type):
        return True
    if hasattr(value, "__call__"):
        logger.error("not expected")
    return False
