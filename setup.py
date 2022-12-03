# encoding: utf-8
# THIS FILE IS AUTOGENERATED!
from __future__ import unicode_literals
from setuptools import setup

setup(
    author="Kyle Lahnakoski",
    author_email="kyle@lahnakoski.com",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="More Streams! Chained function calls",
    extras_require={"tests": ["mo-files", "mo-logs", "zstandard"]},
    include_package_data=True,
    install_requires=["mo-dots", "mo-future==6.230.22310"],
    license="MPL 2.0",
    long_description=(
        "# More Streams\n\nCan code be more elegant with method chaining?\n\n"
    ),
    long_description_content_type="text/markdown",
    name="mo-streams",
    packages=["mo_streams"],
    url="https://github.com/klahnakoski/mo-streams",
    version="0.6.22326",
    zip_safe=False,
)
