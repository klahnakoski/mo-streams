# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from io import BytesIO
from typing import Tuple

from mo_files import File
from mo_imports import delay_import


from mo_streams.utils import Reader, StreamFile, chunk_bytes

StringStream = delay_import("mo_streams.string_stream.StringStream")
ObjectStream = delay_import("mo_streams.object.ObjectStream")


class ByteStream:
    def __init__(self, chunks):
        self._chunks = chunks

    def close(self):
        self._chunks.close()

    def tar(self):
        """
        return a stream of files
        """
        import tarfile
        tf = tarfile.open(mode="r:", fileobj=Reader(self._chunks))

        def file(info):
            reader = tf.extractfile(info)
            if reader is None:
                # directories
                return StreamFile(
                    info.name,
                    lambda: None
                )
            else:
                return StreamFile(
                    info.name,
                    lambda: ByteStream(chunk_bytes(tf.extractfile(info)))
                )

        def read():
            while True:
                info = tf.next()
                if not info:
                    return
                yield file(info)
        return ObjectStream(read(), file(tf.firstmember), StreamFile)

    def zip(self):
        """
        return a stream of files
        """
        import zipfile

        archive = zipfile.ZipFile(self._chunks, mode="r")
        names = archive.namelist()
        return ObjectStream(
            ((ByteStream(archive.open(name, "r")), name) for name in names),
            (ByteStream(BytesIO(b"")), ""),
            Tuple[ByteStream, str],
        )

    def zst(self):
        import zstandard

        def read():
            cctx = zstandard.ZstdCompressor()
            chunker = cctx.chunker(chunk_size=32768)
            for data in self._chunks:
                yield from chunker.compress(data)
            yield from chunker.finish()

        return ByteStream(read())

    def utf8(self):
        def read():
            for data in self._chunks:
                yield data.decode("utf8")

        return StringStream(read())

    def lines(self):
        return self.utf8.lines

    def write(self, file):
        file = File(file)
        with open(file.abspath, "wb") as f:
            for d in self._chunks:
                f.write(d)

    def to_bytes(self):
        return b"".join(self._chunks)

