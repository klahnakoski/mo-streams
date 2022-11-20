

class TupleStream:
    def __init__(self, values, example, datatype):
        self._iter: Iterator[Tuple[Any]] = values
        self._example = example
        self._type: Type = datatype

    def __getattr__(self, item):
        if hasattr(ObjectStream, item):
            Log.error("ambigious")

        accessor = getattr(self._example, item)
        method = getattr(self._type, item)
        if type(method).__name__ in {"method_descriptor", "function"}:
            return MethodStream(
                ((getattr(v, item), *r) for v, *r in self._iter),
                accessor,
                type(accessor),
            )
        else:
            return ObjectStream(
                ((getattr(v, item), *r) for v, *r in self._iter),
                accessor,
                type(accessor),
            )

    def map(self, accessor):
        if isinstance(accessor, str):
            return ObjectStream((getattr(v, accessor) for v in self._iter), Any)
        if accessor in self._type:
            return ObjectStream((accessor(v) for v in self.__iter), Any)

    def to_dict(self):
        return {k: v for v, k in self._iter}

