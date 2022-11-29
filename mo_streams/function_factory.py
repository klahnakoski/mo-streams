import inspect
from types import FunctionType

from mo_logs import logger

from mo_streams.type_utils import Typer, LazyTyper

_get = object.__getattribute__
_set = object.__setattr__


class FunctionFactory:

    def __init__(self, builder, type_):
        if type_ is None:
            logger.error("expecting type")
        _set(self, "build", builder)
        _set(self, "type_", type_)

    def __getattr__(self, item):
        def builder(type_, _schema):
            s = _get(self, "build")(type_, _schema)
            if item in _schema:
                def func(v, a):
                    return a[item]
                return func
            elif isinstance(item, FunctionFactory):
                i = item.build(type_, _schema)
                return lambda v, a: getattr(s(v, a), i(v, a))
            else:
                return lambda v, a: getattr(s(v, a), item)

        return FunctionFactory(builder, getattr(_get(self, "type_"), item))

    def __radd__(self, other):
        def builder(type_, _schema):
            return lambda v, a: other + a

        type_ = Typer(example=other) + _get(self, "type_")
        return FunctionFactory(builder, type_)



    def __call__(self, *args, **kwargs):
        args = [factory(a) for a in args]
        kwargs = {k: factory(v) for k, v in kwargs}

        def builder(type_, _schema):
            s = self.build(type_, _schema)
            _args = [a.build(type_, _schema) for a in args]
            _kwargs = {k: v.build(type_, _schema) for k, v in kwargs.items()}

            def func(v, a):
                return s(v, a)(
                    *(f(v, a) for f in _args),
                    **{k: f(v, a) for k, f in _kwargs.items()}
                )
            return func

        return FunctionFactory(builder, self.type_)


def factory(item, type_=None):
    if isinstance(item, str):
        def builder(type_, _schema):
            return lambda v, a: getattr(v, item)
        return FunctionFactory(builder, getattr(type_, item))
    elif isinstance(item, FunctionFactory):
        return item
    else:
        normalized_func = wrap_func(item)
        def builder3(type_, _schema):
            return normalized_func
        return FunctionFactory(builder3, type_)


def build(item):
    if isinstance(item, FunctionFactory):
        return item.build

    def builder(type_, _schema):
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
        # USE ONLY FIRST PARAMETER
        num_args = len(spec.args) - 1  # ASSUME self IS FIRST ARG
        if num_args == 0:
            def wrap_init0(val, ann):
                return func()
            return wrap_init0
        else:
            def wrap_init1(val, ann):
                return func(val)
            return wrap_init1
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
            return func()
        wrapper = wrapper0
    elif num_args == 1:
        def wrapper1(val, ann):
            return func(val)
        wrapper = wrapper1
    else:
        return func

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
        return factory(value, LazyTyper())


it = factory(lambda v, a: v, LazyTyper())
it.__class__ = TopFunctionFactory

