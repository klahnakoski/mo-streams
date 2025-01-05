# FunctionFactory called "it"

The FunctionFactory is used to assemble a chain of builders, those builders are 
used to construct the generators that operate on the members of a stream.

The factory includes type information and description to help identify coding 
errors; it is effectively "compiling" the generator chain. 

> Kotlin uses `it` as the default lambda expression parameter 

## Examples

FunctionFactories are meant to define simple expressions, and be simpler than 
lambdas.  This simplification is only possible because `Streams` have the 
required type information and call the `.build()` method.  We show examples 
in the stream context where `.build()` is not required


|     FunctionFactory   |       Lambda         |
|-----------------------|----------------------|
|   `it + 2`            |  `lambda x: x+2`     |
|   `it.name`           |  `lambda x: x.name`  |
|   `it(sum)(1, 2)`     |  `lambda: sum(1, 2)` |

The last example shows how we must convert a function (`sum`) into a FunctionFactory;
`it` is a function that can convert objects into function factories so they may 
be further composed.  

## Using `it` for attachment access 

The `map` function will accept FunctionFactories, which can be used to access the
attachments in a stream:

    stream([1, 2, 3])
    .attach(key="example " + it)
    .map(it.key)
    
Note that `.map(lambda x: x.key)` will fail because integers do not have a `key` attribute, while `it` will use attachments.
