# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from unittest import TestCase

from mo_files import File

from mo_streams import stream


class TestStream(TestCase):

    def test_encode(self):
        result = stream("this is a test").encode("utf8").to_bytes()
        self.assertEquals(result, b"this is a test")

    def test_zst(self):
        tempfile = File("tests/resources/test.zst")
        stream("this is a test").utf8().zst().write(tempfile)
        result = tempfile.content().utf8().to_str()
        self.assertEqual(result, "this is a test")

    def test_tar(self):
        tempfile = File("tests/resources/so_queries.tar.zst")
        content = tempfile.content().tar().content().exists().utf8().to_str().to_list()
        self.assertEqual(len(content), 6191)

    def test_zip(self):

        pass # File("tests").leaves()