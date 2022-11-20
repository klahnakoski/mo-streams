# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots.lists import is_many
from mo_future import first

from mo_streams.byte_stream import ByteStream
from mo_streams.empty_stream import EmptyStream
from mo_streams.object_stream import ObjectStream
from mo_streams.string_stream import StringStream


def stream(value):
    if is_many(value):
        example = first(value)
        return ObjectStream(iter(value), example, type(example))
    elif isinstance(value, str):
        return StringStream(iter([value]))
    else:
        return ObjectStream(iter([value]), value, type(value))

