# More Streams

Can Python code be more elegant with method chaining?

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
 
