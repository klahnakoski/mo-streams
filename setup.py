# encoding: utf-8
# THIS FILE IS AUTOGENERATED!
from __future__ import unicode_literals
from setuptools import setup
setup(
    author='Kyle Lahnakoski',
    author_email='kyle@lahnakoski.com',
    classifiers=["Development Status :: 3 - Alpha","Topic :: Software Development :: Libraries","Topic :: Software Development :: Libraries :: Python Modules","License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)","Programming Language :: Python :: 3.8","Programming Language :: Python :: 3.9"],
    description='More Streams! Chained function calls',
    extras_require={"tests":["mo-files","mo-logs","zstandard","boto3","moto","pandas","mo-threads"]},
    include_package_data=True,
    install_requires=["mo-dots==9.300.22349","mo-files==6.318.22362","mo-future==7.298.22349","mo-json==6.310.22362"],
    license='MPL 2.0',
    long_description='# More Streams!!\n\n[![PyPI Latest Release](https://img.shields.io/pypi/v/mo-streams.svg)](https://pypi.org/project/mo-streams/)\n[![Build Status](https://app.travis-ci.com/klahnakoski/mo-streams.svg?branch=master)](https://travis-ci.com/github/klahnakoski/mo-streams)\n [![Coverage Status](https://coveralls.io/repos/github/klahnakoski/mo-streams/badge.svg?branch=dev)](https://coveralls.io/github/klahnakoski/mo-streams?branch=dev)\n[![Downloads](https://pepy.tech/badge/mo-streams/month)](https://pepy.tech/project/mo-streams)\n\n\nPython code is more elegant with method chaining!\n\n\n## Overview\n\nThere are two families of "streams" in this library, both are lazy:\n\n1. `ByteStream` - a traditional stream of bytes intended to pipe bytes through various byte transformers, like compression, encoding and encyrption.  \n2. `ObjectStream`: An iterator/generator with a number of useful methods.\n\n### Example\n\nIn this case I am iterating through all files in a tar and parsing them:\n\n    results = (\n        File("tests/so_queries/so_queries.tar.zst")\n        .content()\n        .content()\n        .exists()\n        .utf8()\n        .to_str()\n        .map(parse)\n        .to_list()\n    )\n    \n Each of the steps constructs a generator, and no work is done until the last step\n \n \n * `File().content()` - will unzst and untar the file content to an `ObjectStream` of file-like objects.  It is short form for `stream(File().read_bytes()).from_zst().from_tar()`\n * The second `.content()` is applied to each of the file-like objects, returning `ByteStream` of the content for each\n * `.exists()` - some of the files (aka directories) in the tar file do not have content, we only include content that exists.\n * `.utf8` - convert to a `StringStream`\n * `.to_str` - convert to a Python `str`, we trust the content is not too large\n * `.map(parse)` - run the parser on each string\n * `.to_list()` - a "terminator", which executes the chain and returns a Python `list` with the results\n \n## Project Status\n\nAlive and in use, but \n\n* basic functions missing\n* inefficient - written using generators\n* generators not properly closed\n\n\n## Optional Reading\n\nThe method chaining style has two distinct benefits\n\n* functions are in the order they are applied \n* intermediate values need no temporary variables\n\nThe detriments are the same that we find in any declarative language: Incorrect code can be difficult to debug because you can not step through it to isolate the problem.  For this reason, the majority of the code in this library is dedicated to validating the links in the function chain before they are run.\n\n### Lessons\n\nThe function chaining style, called "streams" in Java or "linq" in C#, leans heavly on the strict typed nature of those langauges.  This is missing in Python, but type annotations help support this style of programming.\n\n',
    long_description_content_type='text/markdown',
    name='mo-streams',
    packages=["mo_streams"],
    url='https://github.com/klahnakoski/mo-streams',
    version='1.324.22362',
    zip_safe=False
)