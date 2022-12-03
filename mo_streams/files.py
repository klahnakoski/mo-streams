# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_files import File
from mo_future import get_function_name

from mo_streams import ByteStream
from mo_streams._utils import os_path

DECODERS = {
    "zst": ByteStream.from_zst,
    "tar": ByteStream.from_tar,
    "zip": ByteStream.from_zip,
    # "gzip": gzip_stream
}


def _get_file_stream(file, stream):
    """
    RETURN A STREAM FROM THE GIVEN FILE
    :param file:
    :return:
    """
    name, extension = _get_extension(file)
    decoder = DECODERS.get(extension, None)
    if not decoder:
        return stream
    return _get_file_stream(name, decoder(stream))


def _extend(cls):
    """
    DECORATOR TO ADD METHODS TO CLASSES
    :param cls: THE CLASS TO ADD THE METHOD TO
    :return:
    """

    def extender(func):
        setattr(cls, get_function_name(func), func)
        return func

    return extender


@_extend(File)
def content(self):
    return _get_file_stream(self.abspath, ByteStream(open(os_path(self), "rb")))


def _get_extension(file_name):
    parts = file_name.split(".")
    if len(parts) > 1:
        name = ".".join(parts[:-1])
        extension = parts[-1]
        return name, extension
    return file_name, ""
