import inspect


class Typer:
    """
    Handle the lumps of python type manipulation
    """

    def __init__(self, example=None, clazz=None):
        self.example = example
        self.clazz = clazz

    def __getattr__(self, item):
        inspect.getA