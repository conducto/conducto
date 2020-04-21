"""
The goal is to promote semi-structured exceptions so that code like:

    raise Exception('something specific with data={}'.format(data))

becomes more like:

    raise CustomException('somewhat generic', data=data)

This supports exception aggregating in the following way:

    0) Level-0 grouping based on the exception class name.
    1) Level-1 grouping based on the exception message.

The data of the exception, as a general rule, is not included in the `args`
attribute of the exception (which also plays nicely with Sentry aggregations),
but the data is programmatically accessible and included in the string
representation of the exception.

"""
import sys
import pprint


EXC_CTX_KEY = "_ctxStack"


class KwargException(Exception):
    """
    Generic exception class, supporting a message and kwargs only.

    Examples
    --------
    Define a custom exception:

        >>> class MyException(KwargException):
        ...     defaultMessage = 'This is bad'
        ...

    Usage with no data:

        >>> e = MyException()
        >>> e.args
        ('This is bad',)
        >>> e.data
        {}
        >>> str(e)
        MyException: This is bad
        >>> raise e
        MyException: This is bad

    Usage with a custom message and data:

        >>> e = MyException('This is really bad', badBecause='Monday')
        >>> e.args
        ('This is really bad',)
        >>> e.data
        {'badBecause': 'Monday'}
        >>> raise e
        MyException: This is really bad

        Context:

            {'badBecause': 'Monday'}

    Example showing how to stack context information:

        >>> try:
        ...     try:
        ...         raise MyException('Epic failure!', a=1, b=2)
        ...     except MyException as exc0:
        ...         appendContext(exc0, b=3, c=4)
        ...         raise exc0
        ... except MyException as exc1:
        ...    appendContext(exc1, c=5, e=6)
        ...    raise exc1
        ...
        MyException: Epic failure!

        First Context:

            {'a': 1, 'b': 2}

        Stacked Contexts:

            [{'b': 3, 'c': 4}, {'c': 5, 'e': 6}]

        >>> exc1.data
        {'a': 1, 'b': 2}

        >>> exc1.contextStack()
        [{'a': 1, 'b': 2}, {'b': 3, 'c': 4}, {'c': 5, 'e': 6}]

    """

    defaultMessage = None

    def __init__(self, message=None, **kwargs):
        """
        Initialize exception.

        """
        super(KwargException, self).__init__(message)

        self.message = message
        setattr(self, EXC_CTX_KEY, [kwargs])

    def __str__(self):
        return self.toString()

    def contextStack(self):
        return getContextStack(self)

    @property
    def data(self):
        return self.contextStack()[0]

    def toString(self, includeContexts=True):
        """
        Returns a string representation of the exception with contexts included.

        """
        if self.message is None:
            message = str(self.defaultMessage)
        else:
            message = str(self.message)

        stack = self.contextStack()
        firstContext, stackedContexts = stack[0], stack[1:]

        if not includeContexts:
            return message

        tab = " " * 4
        sep = "\n{}".format(tab)
        indented = lambda t: tab + sep.join(pprint.pformat(t).splitlines())

        # Until Python 3.6, kwargs does not maintain specification order

        parts = [message]

        if firstContext or stackedContexts:
            parts.extend(
                [
                    "First Context:" if stackedContexts else "Context:",
                    indented(firstContext),
                ]
            )

        if stackedContexts:
            parts.extend(
                ["Stacked Contexts:", indented(stackedContexts),]
            )

        return "\n\n".join(parts)


def appendContext(exc, **kwargs):
    """
    Appends context to the exception.

    This should *only* be called within an except clause (and only once per
    clause).

    Until Python 3.6, kwargs does not maintain specification order. If ordered
    kwargs are truly needed, consider extending this and KwargException to
    query for a 'kwargs' keyword that, if specified, would supercede the kwargs
    passed into this function.

    """
    if not isinstance(exc, BaseException):
        msg = "exc must be an exception type. Found: {}".format(type(exc))
        raise TypeError(msg)

    if not hasattr(exc, EXC_CTX_KEY):
        setattr(exc, EXC_CTX_KEY, [])

    if kwargs is not None:
        getattr(exc, EXC_CTX_KEY).append(kwargs)


def getContextStack(exc=None):
    """
    Retrieve exception context stack.

    """
    if exc is None:
        _, exc, _ = sys.exc_info()

    return getattr(exc, EXC_CTX_KEY, None)


def _demoTracebacks():
    """
    Despite looks, a() and b() do not generate identical tracebacks.

    The inlining that a() does ends up not adding any context to the traceback,
    whereas the compatiblity function provided by future.utils does.

    Output
    ------
    Traceback (most recent call last):
      File "corinth/tools/exceptions.py", line 214, in _demoTracebacks
        a()
      File "corinth/tools/exceptions.py", line 198, in a
        1/0
    ZeroDivisionError: integer division or modulo by zero




    Traceback (most recent call last):
      File "corinth/tools/exceptions.py", line 221, in _demoTracebacks
        b()
      File "corinth/tools/exceptions.py", line 211, in b
        raise e2
      File "corinth/tools/exceptions.py", line 209, in b
        raise e1
      File "corinth/tools/exceptions.py", line 207, in b
        1/0
    ZeroDivisionError: integer division or modulo by zero

    """
    import sys
    import traceback

    def a():
        try:
            try:
                1 / 0
            except Exception as e1:
                raise e1.with_traceback(sys.exc_info()[2])
        except Exception as e2:
            raise e2.with_traceback(sys.exc_info()[2])

    def b():
        try:
            try:
                1 / 0
            except Exception as e1:
                raise e1
        except Exception as e2:
            raise e2

    try:
        a()
    except Exception:
        traceback.print_exc()

    print("\n\n\n")

    try:
        b()
    except Exception:
        traceback.print_exc()
