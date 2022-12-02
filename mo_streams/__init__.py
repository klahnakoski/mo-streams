# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_dots import Data
from mo_dots.lists import is_many
from mo_future import first
from mo_imports import export
from mo_json import JxType, JX_TEXT

from mo_streams.byte_stream import ByteStream
from mo_streams.empty_stream import EmptyStream
from mo_streams.files import content
from mo_streams.object_stream import ObjectStream
from mo_streams.string_stream import StringStream
from mo_streams.type_utils import Typer
from mo_streams.utils import Stream
from mo_streams.function_factory import it


def stream(value):
    if isinstance(value, (dict, Data)):
        kv = first(value.items())
        if not kv:
            return EmptyStream()
        _, example = kv
        return ObjectStream(
            ((v, {"key": k}) for k, v in value.items()),
            Typer(example=example),
            JxType(key=JX_TEXT),
        )
    elif isinstance(value, str):
        return StringStream(iter([value]))
    elif value == None:
        return EmptyStream
    elif isinstance(value, Stream):
        return value
    elif is_many(value):
        example = first(value)
        return ObjectStream(((v, {}) for v in value), Typer(example=example), JxType())
    else:
        return ObjectStream(iter([(value, {})]), Typer(example=value), JxType())


export("mo_streams.object_stream", stream)
