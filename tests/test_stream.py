# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import os
from unittest import TestCase, skipIf, skip

import boto3
from mo_files import File, TempFile
from mo_math import randoms
from mo_times import Date, YEAR
from moto import mock_s3
from pandas import DataFrame

from mo_streams import stream, it, ANNOTATIONS, Typer, EmptyStream
from mo_streams._utils import Writer
from mo_streams.files import File_usingStream

IS_TRAVIS = bool(os.environ.get("TRAVIS"))


class TestStream(TestCase):
    def test_encode(self):
        "".encode("utf8")
        result = stream("this is a test").encode("utf8").to_bytes()
        self.assertEqual(result, b"this is a test")

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
        content = file.content().rel_path.to_list()
        self.assertEqual(content, ["LICENSE", "README.md"])

    def test_dict_zip(self):
        values = ["a", "b", "c"]
        result = stream(values).enumerate().to_dict()
        self.assertEqual(result, {0: "a", 1: "b", 2: "c"})

    def test_to_zip(self):
        with TempFile(f"delete_{randoms.base64(5)}.zip") as file:
            stream(File("tests").leaves).to_zip().write(file)

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

    def test_limit_under(self):
        result = stream(range(200)).limit(10).to_list()
        self.assertEqual(result, list(range(10)))

    def test_limit_over(self):
        result = stream(range(5)).limit(10).to_list()
        self.assertEqual(result, list(range(5)))

    def test_limit_match(self):
        result = stream(range(10)).limit(10).to_list()
        self.assertEqual(result, list(range(10)))

    def test_to_zip2(self):
        result = (
            stream({"data": [{"a": 1, "b": 2}]})
            .map(DataFrame)
            .attach(writer=it(Writer)())
            .map(it.to_csv(it.writer))
            .attach(name="test_" + it.key)
            .map(it(File_usingStream)(it.name, it.writer.content))
            .to_zip()
            .write("tests/resources/test_to_zip.zip")
        )

    @mock_s3
    def test_zip_to_s3(self):
        s3 = boto3.resource("s3")
        bucket_name = "bucket-" + randoms.hex(10)
        bucket = s3.create_bucket(Bucket=bucket_name)

        result = (
            stream({"data": [{"a": 1, "b": 2}]})
            .map(DataFrame)
            .attach(writer=it(Writer)())
            .map(it.to_csv(it.writer, index=False, line_terminator="\n"))
            .attach(name="test_" + it.key)
            .map(it(File_usingStream)(it.name, it.writer.content))
            .to_zip()
            .to_s3(name="test", bucket=bucket_name)
        )

        # bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            key, body = obj.key, obj.get()["Body"].read()
            if key == "test":
                content = stream(body).from_zip().content().utf8().to_str().to_list()
                self.assertEqual(content, ["a,b\n1,2\n"])

    @skip("not sure why this is an error")
    def test_missing_lambda_parameter(self):
        with self.assertRaises(Exception):
            result = (
                stream({"data": [{"a": 1, "b": 2,}]})
                .attach(writer=lambda: Writer())
                .map(it.writer.write(str(it)))
                .to_list()
            )
            print(result)

    def test_map_int(self):
        result = stream(["1", "2", "3"]).map(int).to_list()
        self.assertEqual(result, [1, 2, 3])

    def test_map_function(self):
        result = stream(["1", "2", "3"]).map(length).to_list()
        self.assertEqual(result, [1, 1, 1])

    def test_map_lambda(self):
        result = stream(["1", "2", "3"]).map(lambda v: int(v) + 1).to_list()
        self.assertEqual(result, [2, 3, 4])

    def test_filter(self):
        result = stream([1, 2, 3]).filter(lambda v: v % 2).to_list()
        self.assertEqual(result, [1, 3])

    def test_group1(self):
        result = (
            stream([1, 2, 3])
            .group(lambda v: v % 2)
            .map(lambda v, a: {"group": a["group"], "value": v.to_list()})
            .to_list()
        )
        self.assertEqual(
            result, [{"group": 0, "value": [2]}, {"group": 1, "value": [1, 3]}]
        )

    def test_group2(self):
        result = (
            stream([1, 2, 3])
            .group(lambda v: v % 2)
            .map(it.sum())
            .map(lambda v, a: {"group": a["group"], "value": v})
            .to_list()
        )
        self.assertEqual(result, [{"group": 0, "value": 2}, {"group": 1, "value": 4}])

    def test_first(self):
        result = stream([1, 2, 3]).first()
        self.assertEqual(result, 1)

    def test_map_it(self):
        class SomeClass:
            num = 0

            def __init__(self):
                self.value = SomeClass.num
                SomeClass.num += 1

        ANNOTATIONS[(SomeClass, "value")] = Typer(python_type=int)
        result = stream([SomeClass(), SomeClass(), SomeClass()]).map(it.value).last()
        self.assertEqual(result, 2)

    @skip("not implemented yet")
    def test_deep_iteration(self):
        class Something:
            def __init__(self, value):
                self.value = value

        value = Something({"props": [{"a": 1}, {"a": 2}, {"a": 3}]})
        result = stream(value).value.props.a.to_list()
        self.assertEqual(result, [1, 2, 3])

    @skip("not implemented yet")
    def test_reverse_dict(self):
        data = {1: "a", 2: "b", 3: "c", 4: "a"}
        result = stream(data).group(it).map(it.key.to_list()).to_dict(key="group")
        self.assertEqual(result, {"a": [1, 4], "b": [2], "c": ["3"]})

    @skip("not implemented yet")
    def test_pivot(self):
        populations = [
            {"date": "2019-07-01", "cohort": "00", "population": 1000},
            {"date": "2019-07-01", "cohort": "45", "population": 2000},
            {"date": "2020-07-01", "cohort": "00", "population": 3000},
            {"date": "2020-07-01", "cohort": "45", "population": 4000},
            {"date": "2021-07-01", "cohort": "00", "population": 5000},
            {"date": "2021-07-01", "cohort": "45", "population": 6000},
        ]

        last_year = "2020-07-01"
        this_year = "2021-07-01"
        next_year = (Date(this_year) + YEAR).format("%Y-%m-%d")
        years = [this_year, last_year, next_year]
        result = (
            stream(populations)
            # select records we need
            .filter(it.date in years)
            # transpose (pivot date to rows)
            .pivot(values="population", index="cohort", columns="date")
            # calc
            .attach(2 * it[this_year] - it[last_year], next_year)
            # melt dates back to rows
            .melt(
                id_vars=["cohort"],
                value_vars=years,
                var_name="date",
                value_name="population",
            )
            # add to original table
            .append_to(populations)
        )
        expected = [
            {"date": "2019-07-01", "cohort": "00", "population": 1000},
            {"date": "2019-07-01", "cohort": "45", "population": 2000},
            {"date": "2020-07-01", "cohort": "00", "population": 3000},
            {"date": "2020-07-01", "cohort": "45", "population": 4000},
            {"date": "2021-07-01", "cohort": "00", "population": 5000},
            {"date": "2021-07-01", "cohort": "45", "population": 6000},
            {"date": "2022-07-01", "cohort": "00", "population": 7000},
            {"date": "2022-07-01", "cohort": "45", "population": 8000},
        ]
        self.assertEqual(result, expected)

    def test_empty_attach(self):
        stream = EmptyStream()
        self.assertIsNone(stream.attach(a=22).sum())

    def test_div(self):
        self.assertEqual(
            stream([1, 2, None, 3]).map(it / 100).to_list(), [0.01, 0.02, None, 0.03]
        )

    def test_radd(self):
        self.assertEqual(stream([1, 2, None, 3]).map(3 + it).to_list(), [4, 5, None, 6])


def length(value):
    return len(value)
