# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import os
from unittest import TestCase, skipIf

from mo_files import File

from mo_streams import stream

IS_TRAVIS = bool(os.environ.get("TRAVIS"))


class TestStream(TestCase):
    def test_encode(self):
        result = stream("this is a test").encode("utf8").to_bytes()
        self.assertEquals(result, b"this is a test")

    def test_zst(self):
        file = File("tests/resources/test.zst")
        stream("this is a test").utf8().to_zst().write(file)
        result = file.content().utf8().to_str()
        self.assertEqual(result, "this is a test")

    @skipIf(IS_TRAVIS, "file not repo")
    def test_tar(self):
        file = File("tests/resources/so_queries.tar.zst")
        content = file.content().content().exists().utf8().to_str().to_list()
        self.assertEqual(len(content), 6191)

    def test_from_zip(self):
        file = File("tests/resources/example.zip")
        content = file.content().name.to_list()
        self.assertEqual(content, ["LICENSE", "README.md"])

    def test_dict_zip(self):
        values = ["a", "b", "c"]
        result = stream(values).enumerate().to_dict()
        self.assertEqual(result, {0: "a", 1: "b", 2: "c"})

    def test_to_zip(self):
        file = File("delete_me.zip")
        stream(File("tests").leaves).to_zip().write(file)
        file.delete()

    def test_lambda(self):
        def func(x):
            return x + 2

        result = stream([1, 2, 3]).map(func).to_list()
        self.assertEqual(result, [3, 4, 5])

    def test_sort(self):
        result = stream([2, 3, 1]).sort().to_list()
        self.assertEqual(result, [1, 2, 3])

    def test_reverse(self):
        result = stream([2, 3, 1]).reverse().to_list()
        self.assertEqual(result, [1, 3, 2])

    def test_distinct(self):
        result = stream([2, 2, 2, 1, 4, 3, 1]).distinct().to_list()
        self.assertEqual(result, [2, 1, 4, 3])
