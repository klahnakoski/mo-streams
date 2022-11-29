import inspect

from mo_logs import logger


class Typer:
    """
    Handle the lumps of python type manipulation
    """
    def __init__(self, *, example=None, type_=None, function=None):
        if function:
            # find function return type
            inspect.signature(function)
        elif example:
            self.type_ = type(example)
        else:
            self.type_ = type_

    def __getattr__(self, item):
        print("hi")
        inspect.getmembers(self.type_)

    def __add__(self, other):
        if self.type_ is str or other.type_ is str:
            return Typer(type_=str)
        logger.error("not handled")

    def __str__(self):
        return f"Typer(class={self.type_.__name__})"


class LazyTyper(Typer):
    """
    PLACEHOLDER FOR STREAM ELEMENT TYPE, UNKNOWN DURING LAMBDA DEFINTION
    """

    def __init__(self, resolver=None):
        Typer.__init__(self)
        self._resolver : Typer = resolver or (lambda t: t)

    def __getattr__(self, item):
        def build(type_):
            return getattr(type_, item)
        return LazyTyper(build)

    def __str__(self):
        return "LazyTyper()"

