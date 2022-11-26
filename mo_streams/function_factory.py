import inspect
from types import FunctionType

from mo_streams.utils import is_function

_get = object.__getattribute__
_set = object.__setattr__


class FunctionFactory:

    def __init__(self, builder):
        self.build = builder

    def __getattr__(self, item):
        def builder(_example, _type, _schema):
            s = self.build(_example, _type, _schema)
            if item in _schema:
                def func(v, a):
                    return a[item]
                return func
            elif isinstance(item, FunctionFactory):
                i = item.build(_example, _type, _schema)
                return lambda v, a: getattr(s(v, a), i(v, a))
            else:
                return lambda v, a: getattr(s(v, a), item)

        return FunctionFactory(builder)

    def __call__(self, *args, **kwargs):
        args = [factory(a) for a in args]
        kwargs = {k: factory(v) for k, v in kwargs}

        def builder(_example, _type, _schema):
            s = self.build(_example, _type, _schema)
            _args = [a.build(_example, _type, _schema) for a in args]
            _kwargs = {k: v.build(_example, _type, _schema) for k, v in kwargs.items()}
            def func(v, a):
                return s(v, a)(
                    *(f(v, a) for f in _args),
                    **{k: f(v, a) for k, f in _kwargs.items()}
                )
            return func

        return FunctionFactory(builder)


def factory(item):
    if isinstance(item, FunctionFactory):
        return item
    elif is_function(item):
        def builder1(_example, _type, _schema):
            return wrap_func()
        return FunctionFactory(builder1)
    else:
        def builder2(_example, _type, _schema):
            return lambda v, a: item
        return FunctionFactory(builder2)


def build(item):
    if isinstance(item, FunctionFactory):
        return item.build

    def builder(_example, _type, _schema):
        return lambda v, a: item

    return builder


# build list of single arg builtins, that can be used as parse actions
singleArgBuiltins = [
    sum,
    len,
    sorted,
    reversed,
    list,
    tuple,
    set,
    any,
    all,
    min,
    max,
]

singleArgTypes = [
    int,
    float,
    str,
    bool,
    complex,
    dict,
]


def wrap_func(func):
    if func in singleArgBuiltins:
        spec = inspect.getfullargspec(func)
    elif func.__class__.__name__ == "staticmethod":
        func = func.__func__
        spec = inspect.getfullargspec(func)
    elif func.__class__.__name__ == "builtin_function_or_method":
        spec = inspect.getfullargspec(func)
    elif func in singleArgTypes:
        spec = inspect.FullArgSpec(["value"], None, None, None, [], None, {})
    elif isinstance(func, type):
        spec = inspect.getfullargspec(func.__init__)
        func = func.__call__
    elif isinstance(func, FunctionType):
        spec = inspect.getfullargspec(func)
    elif hasattr(func, "__call__"):
        spec = inspect.getfullargspec(func)

    if spec.varargs:
        num_args = 3
    elif spec.args and spec.args[0] in ["cls", "self"]:
        num_args = len(spec.args) - 1
    else:
        num_args = len(spec.args)

    if num_args == 0:
        def wrapper0(val, ann):
            return func(), ann
        wrapper = wrapper0
    elif num_args == 1:
        def wrapper1(val, ann):
            return func(val), ann
        wrapper = wrapper1
    else:
        def wrapper2(val, ann):
            return func(val, ann), ann
        wrapper = wrapper2

    # copy func name to wrapper for sensible debug output
    try:
        func_name = getattr(func, "__name__", getattr(func, "__class__").__name__)
    except Exception:
        func_name = str(func)
    wrapper.__name__ = func_name

    return wrapper


class TopFunctionFactory(FunctionFactory):
    """
    it(x)  RETURNS A FunctionFactory FOR x
    """
    def __call__(self, value):
        def builder(_example, _type, _schema):
            return lambda v, a: value

        return FunctionFactory(builder)


it = factory(lambda v: v)
it.__class__ = TopFunctionFactory
