# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from io import BytesIO

from mo_files import File
from mo_imports import delay_import

from mo_streams.utils import chunk_bytes, File_usingStream

StringStream = delay_import("mo_streams.string_stream.StringStream")
ObjectStream = delay_import("mo_streams.object_stream.ObjectStream")
tar_stream = delay_import("mo_streams.file_stream.tar_stream")
zip_stream = delay_import("mo_streams.file_stream.zip_stream")


class ByteStream:
    def __init__(self, reader):
        self.reader: BytesIO = reader

    def close(self):
        self.reader.close()

    def from_zip(self):
        """
        RETURN A STREAM OF Files
        """
        import zipfile

        def read():
            archive = zipfile.ZipFile(self.reader, mode="r")
            for info in archive.filelist:
                yield File_usingStream(info.filename, ByteStream(archive.open(info.filename, "r")))

        return ObjectStream(
            read(),
            File_usingStream("", ByteStream(None)),
            File_usingStream
        )

    def from_zst(self):
        """
        :param stream:
        :return:
        """
        import zstandard

        stream_reader = (
            zstandard
            .ZstdDecompressor(max_window_size=2147483648)
            .stream_reader(self.reader)
        )
        return ByteStream(stream_reader)

    def from_tar(self):
        """
        return a stream of files
        """
        import tarfile

        tf = tarfile.open(mode="r:", fileobj=self.reader)

        def file(info):
            reader = tf.extractfile(info)
            if reader is None:
                # directories
                return File_usingStream(info.name, lambda: None)
            else:
                return File_usingStream(
                    info.name, lambda: ByteStream(tf.extractfile(info))
                )

        def read():
            while True:
                info = tf.next()
                if not info:
                    return
                yield file(info)

        return ObjectStream(read(), file(tf.firstmember), File_usingStream)

    def to_zst(self):
        from zstandard import ZstdCompressor

        return ByteStream(ZstdCompressor().stream_reader(self.reader))

    def utf8(self):
        def read():
            for data in chunk_bytes(self.reader):
                yield data.decode("utf8")

        return StringStream(read())

    def lines(self):
        return self.utf8.lines

    def write(self, file):
        file = File(file)
        with open(file.abspath, "wb") as f:
            for d in chunk_bytes(self.reader):
                f.write(d)

    def to_bytes(self):
        return b"".join(chunk_bytes(self.reader))
