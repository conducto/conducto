import typing, datetime, collections, inspect, re
from dateutil import parser

Token = typing.NewType("Token", str)
Tag = typing.NewType("Tag", str)
UserId = typing.NewType("UserId", str)
PipelineId = typing.NewType("PipelineId", str)
HostId = typing.NewType("HostId", str)
OrgId = typing.NewType("OrgId", int)
TaskId = typing.NewType("TaskId", str)
InstanceId = typing.NewType("InstanceId", str)


def runtime_type(typ):
    """Find the real, underlying Python type"""
    if isinstance(typ, type):
        return typ
    elif is_NewType(typ):
        return runtime_type(typ.__supertype__)
    elif isinstance(typ, typing._GenericAlias):
        return runtime_type(typ.__origin__)
    # TODO: (kzhang) support for `typing.TypeVar`
    return typ


def is_instance(obj, typ):
    """Instance check against a given typ, which can be a proper "Python" type or a "typing" type"""
    try:
        if typ == inspect._empty or isinstance(typ, typing.TypeVar):
            # TODO: (kzhang) for now, pass all checks against `typing.TypeVar`. To be complete
            # we should check `obj` against `TypeVar.__constraints__`
            return True
        elif isinstance(typ, type):  # ex: `str`
            return isinstance(obj, typ)
        elif isinstance(typ, typing._GenericAlias):  # ex: `typing.List[int]`
            # TODO: (kzhang) add support for typing.Set|Dict|etc. ?
            if typ.__origin__ is typing.Union:
                if len(typ.__args__) == 2 and typ.__args__[1] is type(None):
                    return obj is None or isinstance(obj, typ.__args__[0])
                else:
                    raise TypeError(
                        f"Only Union[T, None] is allowed, i.e., Optional[T], got {typ}"
                    )
            if typ.__origin__ != list:
                raise TypeError(f"Only typing.List[T] is allowed, got {typ}")
            if not isinstance(obj, list):
                return False
            item_type = typ.__args__[0]
            return all(is_instance(o, item_type) for o in obj)
        elif is_NewType(typ):  # ex: `typing.NewType('MyId', str)`
            return is_instance(obj, typ.__supertype__)
        else:
            raise TypeError(f"Invalid type annotation {typ}/{type(type)}")
    except TypeError as e:
        # Hack to make python 3.6 work.
        if str(e).startswith("Parameterized generics cannot be used with class"):
            if typ.__origin__ != typing.List:
                raise TypeError(f"Only typing.List[T] is allowed, got {typ}")
            if not isinstance(obj, list):
                return False
            item_type = typ.__args__[0]
            return all(is_instance(o, item_type) for o in obj)
        else:
            raise e


def is_NewType(typ):
    """Checks whether the given `typ` was produced by `typing.NewType`"""
    # @see typing.py:NewType
    return (
        inspect.isfunction(typ)
        and hasattr(typ, "__name__")
        and hasattr(typ, "__supertype__")
    )


def _serializer(obj):
    if hasattr(obj, "to_str"):
        return obj.to_str
    if isinstance(obj, bytes):
        return obj.decode
    if isinstance(obj, list):
        return lambda: List.join(map(serialize, obj))
    return obj.__str__


def _deserializer(typ):
    if hasattr(typ, "from_str"):
        return typ.from_str
    if typ == bytes:
        return str.encode
    return typ


def serialize(obj):
    return _serializer(obj)()


def deserialize(typ, obj_str):
    if typ is type(None):
        if obj_str is None or isinstance(obj_str, str):
            return obj_str
        raise ValueError(
            f"deserialize with type=NoneType expected None or str, but got {repr(obj_str)}"
        )
    return _deserializer(typ)(obj_str)


# Wrapper types with specialized de-serialization logic. These types cannot
# actually be instantiated, as their `__new__` functions return an instance
# of the type they represent, not themselves. For instance, `Bool('true')`
# returns a `bool`, not a `Bool`.
class Bool(int):  # we cannot subclass `bool`, use next-best option
    def __new__(cls, val):
        return val is not None and str(val).strip().lower() not in [
            "",
            "0",
            "n",
            "none",
            "false",
            "f",
            "0.0",
        ]

    @staticmethod
    def from_str(s):
        return Bool(s)


class Datetime_Date(datetime.date):
    def __new__(cls, date_str):
        assert isinstance(date_str, str), "input is not a string: {} - {}".format(
            date_str, type(date_str)
        )
        # Will work with various types of inputs such as:
        #   - '2019-03-11'
        #   - '20190311'
        #   - 'march 11, 2019'
        dt = parser.parse(date_str)
        if dt.time() != datetime.datetime.min.time():
            raise ValueError(
                "Interpreting input as a date, but got non-zero "
                "time component: {} -> {}".format(date_str, dt)
            )
        return dt.date()

    @staticmethod
    def from_str(s):
        return Datetime_Date(s)


class Datetime_Time(datetime.time):
    def __new__(cls, time_str):
        assert isinstance(time_str, str), "input is not a string: {} - {}".format(
            time_str, type(time_str)
        )
        # Will work with various types of inputs such as:
        #   - '2019-03-11'
        #   - '20190311'
        #   - 'march 11, 2019'
        tm = parser.parse(time_str)
        if tm.date() != datetime.datetime.now().date():
            raise ValueError(
                "Interpreting input as a time, but got non-zero "
                "date component: {} -> {}".format(time_str, tm)
            )
        return tm.time()

    @staticmethod
    def from_str(s):
        return Datetime_Time(s)


class Datetime_Datetime(datetime.datetime):
    def __new__(cls, datetime_str):
        assert isinstance(datetime_str, str), "input is not a string: {} - {}".format(
            datetime_str, type(datetime_str)
        )
        return parser.parse(datetime_str)

    @staticmethod
    def from_str(s):
        return Datetime_Datetime(s)


# NOTE (kzhang):
# When I first wrote this, I thought it'd be nice to have a wrapper type that is a real `type`
# so we can do type comparisons (ex: `issubclass()`). I didn't actually end up
# running type comparisons with these, so this design may be too complex for what
# it provides. Changing it back to the old model is easy, however, and I can
# do that at any time.

# This is a metaclass for dynamically creating `List[T]` class types. Usage:
# - List('12,34') = ['12', '34'] // <class 'list'> (*not* List)
# - issubclass(List, list) = True
#
# - List[int]('12,34') = [12, 34] // <class 'list'> (*not* List)
# - issubclass(List[int], List) = True
#
# - List[123] => err (not a valid type param)
# - List[list] => err (cannot have nested iterables)
# - List[int][str] => err (cannot parameterize again)
class _Meta_List(type):
    _cache = dict()

    def _type_err(err, typ):
        raise TypeError(f"A {List.__name__}[T] {err}. Got T = {typ} - {type(typ)}")

    def __getitem__(cls, typ):  # `typ` is the `T` in `List[T]`
        if cls != List:
            raise TypeError(
                "Cannot parameterized already-parameterized list. Did you call List[S][T]?"
            )
        # TODO (kzhang): Add support for custom typing.X types
        if not isinstance(typ, type):
            _Meta_List._type_err("must be parameterized with a valid Python type.", typ)
        if typ != str and issubclass(typ, collections.abc.Iterable):
            _Meta_List._type_err("cannot be parameterized with an Iterable type.", typ)
        if typ not in cls._cache:
            # using `type` as a programmatic class factory
            cls._cache[typ] = type(
                f"{List.__name__}[{typ.__name__}]",  # name
                (List,),  # parent class
                # TODO: (kzhang) The _de_serializer is hidden under a lambda so that this `List[T]` type
                # does not have a member with type `CmdLineDeserializer`, which would otherwise interfere
                # with the cmd-line parsers when de-serializing this type. The de-serializing function is
                # only intended for the contained items, not the `List[T]` itself.
                dict(_de_serializer=lambda obj_str: _deserializer(typ)(obj_str)),
            )  # attributes
        return cls._cache[typ]


# base list type with configurable de-serializer


class List(list, metaclass=_Meta_List):
    _de_serializer = str

    def __new__(cls, list_str):
        assert isinstance(list_str, str), "list input is not a string: {} - {}".format(
            list_str, type(list_str)
        )
        # cls can be `List` or some `List[T]` if we are using a class created by `_Meta_List`
        res = []
        for token in List.split(list_str):
            try:
                res.append(cls._de_serializer(token))
            except Exception as e:
                raise TypeError(
                    f"An error occured while using the de-serializer "
                    f'{cls._de_serializer} on string "{token}"',
                    e,
                )
        return res

    @staticmethod
    def join(tokens: typing.Iterable) -> str:
        output = []
        for token in tokens:
            output.append(token.replace(LIST_DELIM, "\\" + LIST_DELIM))
        return LIST_DELIM.join(output)

    @staticmethod
    def split(token: str) -> list:
        tokens = re.split(r"(?<!\\),", token)
        output = []
        for token in tokens:
            output.append(token.replace("\\" + LIST_DELIM, LIST_DELIM))
        return output


LIST_DELIM = ","
