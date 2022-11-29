# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import inspect
from types import FunctionType

from mo_logs import logger

from mo_streams.type_utils import Typer, LazyTyper

_get = object.__getattribute__
_set = object.__setattr__


class FunctionFactory:

    def __init__(self, builder, type_, desc):
        if type_ is None:
            logger.error("expecting type")
        _set(self, "build", builder)
        _set(self, "type_", type_)
        _set(self, "_desc", desc)

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

        return FunctionFactory(builder, getattr(_get(self, "type_"), item), f"{self}.{item}")

    def __radd__(self, other):
        def builder(type_, _schema):
            return lambda v, a: other + a

        type_ = Typer(example=other) + _get(self, "type_")
        return FunctionFactory(builder, type_, f"{other} + {self}")

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

        desc_args = [str(a) for a in args]
        desc_args.extend(f"{k}={v}" for k,v in kwargs)
        params = ",".join(desc_args)
        return FunctionFactory(builder, self.type_, f"{self}({params})")

    def __str__(self):
        return _get(self, "_desc")


def factory(item, type_=None):
    if isinstance(item, str):
        def builder(type_, _schema):
            return lambda v, a: getattr(v, item)
        return FunctionFactory(builder, getattr(type_, item), f"{type_}.{item}")
    elif isinstance(item, FunctionFactory):
        return item
    else:
        normalized_func, type_ = wrap_func(item, type_)
        def builder3(type_, _schema):
            return normalized_func
        return FunctionFactory(builder3, type_, f"returning {type_}")


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


def wrap_func(func, type_):
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
        new_func = func.__call__
        # USE ONLY FIRST PARAMETER
        num_args = len(spec.args) - 1  # ASSUME self IS FIRST ARG
        if num_args == 0:
            def wrap_init0(val, ann):
                return new_func()
            return wrap_init0, Typer(type_=func)
        else:
            def wrap_init1(val, ann):
                return new_func(val)
            return wrap_init1, Typer(type_=func)
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
        return func, type_

    # copy func name to wrapper for sensible debug output
    try:
        func_name = getattr(func, "__name__", getattr(func, "__class__").__name__)
    except Exception:
        func_name = str(func)
    wrapper.__name__ = func_name

    return wrapper, type_


class TopFunctionFactory(FunctionFactory):
    """
    it(x)  RETURNS A FunctionFactory FOR x
    """
    def __call__(self, value):
        return factory(value, LazyTyper())

    def __str__(self):
        return "it"


it = factory(lambda v, a: v, LazyTyper())
it.__class__ = TopFunctionFactory
