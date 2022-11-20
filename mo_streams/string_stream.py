# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots.lists import Log

from mo_streams import ObjectStream
from mo_streams.byte_stream import ByteStream
from mo_streams.utils import Reader


class StringStream:
    def __init__(self, chunks):
        self._chunks = chunks

    def __getattr__(self, item):
        if hasattr(StringStream, item):
            Log.error("ambigious")

        accessor = getattr("", item)
        return ObjectStream(
            (getattr(v, item) for v in self._chunks), accessor, type(accessor)
        )

    def utf8(self):
        return ByteStream(Reader((c.encode("utf8") for c in self._chunks)))

    def lines(self):
        def read():
            line = ""
            end = -1
            try:
                while True:
                    while end == -1:
                        content = next(self._chunks)
                        line += content
                        end = line.find("\n")

                    while end != -1:
                        yield line[:end]
                        line = line[end:]
                        end = line.find("\n")
            except StopIteration:
                if line:
                    yield line

        return ObjectStream(read, "", str)

    def to_str(self):
        return "".join(self._chunks)
