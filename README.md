# More Streams!!

[![PyPI Latest Release](https://img.shields.io/pypi/v/mo-streams.svg)](https://pypi.org/project/mo-streams/)
 [![Build Status](https://github.com/klahnakoski/mo-streams/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/klahnakoski/mo-streams/actions/workflows/build.yml)
 [![Coverage Status](https://coveralls.io/repos/github/klahnakoski/mo-streams/badge.svg?branch=dev)](https://coveralls.io/github/klahnakoski/mo-streams?branch=dev)
[![Downloads](https://pepy.tech/badge/mo-streams/month)](https://pepy.tech/project/mo-streams)


Python code is more elegant with method chaining!


## Overview

There are two families of "streams" in this library, both are lazy:

1. `ByteStream` - a traditional stream of bytes intended to pipe bytes through various byte transformers, like compression, encoding and encyrption.  
2. `ObjectStream`: An iterator/generator with a number of useful methods.

### Example

In this case I am iterating through all files in a tar and parsing them:

    results = (
        File("tests/so_queries/so_queries.tar.zst")
        .content()
        .content()
        .exists()
        .utf8()
        .to_str()
        .map(parse)
        .to_list()
    )
    
 Each of the steps constructs a generator, and no work is done until the last step
 
 
 * `File().content()` - will unzst and untar the file content to an `ObjectStream` of file-like objects.  It is short form for `stream(File().read_bytes()).from_zst().from_tar()`
 * The second `.content()` is applied to each of the file-like objects, returning `ByteStream` of the content for each
 * `.exists()` - some of the files (aka directories) in the tar file do not have content, we only include content that exists.
 * `.utf8` - convert to a `StringStream`
 * `.to_str` - convert to a Python `str`, we trust the content is not too large
 * `.map(parse)` - run the parser on each string
 * `.to_list()` - a "terminator", which executes the chain and returns a Python `list` with the results
 
## Project Status

Alive and in use, but 

* basic functions missing
* inefficient - written using generators
* generators not properly closed


## Optional Reading

The method chaining style has two distinct benefits

* functions are in the order they are applied 
* intermediate values need no temporary variables

The detriments are the same that we find in any declarative language: Incorrect code can be difficult to debug because you can not step through it to isolate the problem.  For this reason, the majority of the code in this library is dedicated to validating the links in the function chain before they are run.

### Lessons

The function chaining style, called "streams" in Java or "linq" in C#, leans heavly on the strict typed nature of those langauges.  This is missing in Python, but type annotations help support this style of programming.

