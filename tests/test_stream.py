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
from mo_dots import exists, Data
from mo_files import File, TempFile
from mo_logs import logger
from mo_math import randoms
from mo_testing.fuzzytestcase import add_error_reporting
from mo_threads import start_main_thread, stop_main_thread
from mo_times import Date, YEAR
from moto import mock_s3

from mo_json import json2value
from mo_streams import stream, it, ANNOTATIONS, Typer, EmptyStream, from_s3
from mo_streams._utils import Writer
from mo_streams.files import File_usingStream
from mo_streams.string_stream import line_terminator

IS_TRAVIS = bool(os.environ.get("TRAVIS"))


@add_error_reporting
class TestStream(TestCase):

    @classmethod
    def setUpClass(cls):
        stop_main_thread()
        start_main_thread()

    @classmethod
    def tearDownClass(cls):
        stop_main_thread()


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

    def test_work_on_files(self):
        def analysis(lines):
            ## expect list of Data
            if not isinstance(lines, list):
                logger.error("expecting list")
            if not all(isinstance(d, Data) for d in lines):
                logger.error("expecting Data")
            return lines

        result = (
            File("tests/resources/data_in_zip.zip")
            .content()
            .map(it.content().lines().filter(exists).map(json2value).to_list())
            .map(analysis)
            .to_list()
        )
        self.assertEqual(result, [[{"a": 1}, {"a": 2}, {"a": 3}], [{"c": 11}, {"c": 22}, {"c": 33}]])

    def test_work_on_files_simple(self):
        result = File("tests/resources/data_in_zip.zip").content().map(it.content().lines().to_list()).to_list()
        self.assertEqual(
            result,
            [
                ['{"a":  1}', "", '{"a":  2}', "", '{"a":  3}', "", ""],
                ['{"c":  11}', '{"c":  22}', "", '{"c":  33}', "", ""],
            ],
        )

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

    @skipIf(IS_TRAVIS, "pandas too hard for travis")
    def test_to_zip2(self):
        from pandas import DataFrame

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

    @skipIf(IS_TRAVIS, "pandas too hard for travis")
    @mock_s3
    def test_zip_to_s3(self):
        from pandas import DataFrame

        s3 = boto3.resource("s3")
        bucket_name = "bucket-" + randoms.hex(10)
        bucket = s3.create_bucket(Bucket=bucket_name)

        result = (
            stream({"data": [{"a": 1, "b": 2}]})
            .map(DataFrame)
            .attach(writer=it(Writer)())
            .map(it.to_csv(it.writer, index=False, **{line_terminator: "\n"}))
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

    def test_inequality(self):
        self.assertEqual(stream([1, 2, 3]).filter(it > 2).to_list(), [3])
        self.assertEqual(stream([1, 2, 3]).filter(it >= 2).to_list(), [2, 3])
        self.assertEqual(stream([1, 2, 3]).filter(it < 2).to_list(), [1])
        self.assertEqual(stream([1, 2, 3]).filter(it <= 2).to_list(), [1, 2])

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
            .map(lambda v, att: {"group": att["group"], "value": v.to_list()})
            .to_list()
        )
        self.assertEqual(result, [{"group": 0, "value": [2]}, {"group": 1, "value": [1, 3]}])

    def test_group2(self):
        result = (
            stream([1, 2, 3])
            .group(lambda v: v % 2)
            .map(it.sum())
            .map(lambda v, att: {"group": att["group"], "value": v})
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
            .melt(id_vars=["cohort"], value_vars=years, var_name="date", value_name="population",)
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
        self.assertEqual(stream([1, 2, None, 3]).map(it / 100).to_list(), [0.01, 0.02, None, 0.03])

    def test_radd(self):
        self.assertEqual(stream([1, 2, None, 3]).map(3 + it).to_list(), [4, 5, None, 6])

    def test_rsub(self):
        self.assertEqual(stream([1, 2, None, 3]).map(3 - it).to_list(), [2, 1, None, 0])

    def test_str_startswith(self):
        self.assertEqual(stream(["abc", "def", "ghi"]).filter(it.startswith("a")).to_list(), ["abc"])

    def test_filter_and_concat_naive(self):
        self.assertEqual(stream(["A", "", None, "d"]).filter(exists).map(str.lower).join(","), "a,d")

    def test_filter_and_concat_simpler(self):
        self.assertEqual(stream(["A", "", None, "d"]).exists().lower().join(","), "a,d")

    @mock_s3
    def test_s3_csv_to_dict(self):
        s3 = boto3.resource("s3")
        bucket_name = "bucket-" + randoms.hex(10)
        bucket = s3.create_bucket(Bucket=bucket_name)

        content = "id,name\n1,Alice\n2,Bob"
        bucket.put_object(Key='test.csv', Body=content)

        result = (
            from_s3(key="test.csv", bucket=bucket_name)
            .content()
            .utf8()
            .csv()
            .to_list()
        )

        self.assertEqual(result, [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}])

    def test_from_csv_w_map(self):
        result = (
            stream("id,name\n1,Alice\n2,Bob")
            .csv()
            .map(it['id'] + it['name'])
            .to_list()
        )

        self.assertEqual(result, ["1Alice", "2Bob"])

    def test_count_values_and_sort(self):
        values = [
            "apple",
            "pear",
            "apple",
            "orange",
            "pear",
            "apple"
        ]

        counts = (
            stream(values)
            .group(key=it)
            .map(it.count())
            .sort(reverse=True)
            .map(lambda v, att: {att["key"]: v})
            .to_list()
        )

        self.assertEqual(counts, [{"apple": 3}, {"pear": 2}, {"orange": 1}])

    def test_count_values_and_sort2(self):
        values = [
            "apple",
            "pear",
            "apple",
            "orange",
            "pear",
            "apple"
        ]

        counts = (
            stream(values)
            .group(key=it)
            .map(it.count())
            .to_dict()
        )

        self.assertEqual(counts, {"apple": 3, "pear": 2, "orange": 1})

    def test_use_a_function(self):
        def add_1(values, att) -> int:
            return sum(v['a'] for v in values.to_list())

        result = stream([{"a":1}, {"a":2}, {"a":3}]).group(mode=it['a']%2).map(add_1).to_list()
        self.assertEqual(result, [2, 4])


def length(value):
    return len(value)
