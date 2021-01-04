from . import pipeline


class base:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def to_literal(self):
        d = {}
        for k, v in self.kwargs.items():
            if isinstance(v, pipeline.Node):
                d[k] = str(v)
            else:
                d[k] = v
        return [self.name, d]


def _parse(cb_literal):
    name, cb_kwargs = cb_literal
    cb_func = globals()[name]
    return cb_func(**cb_kwargs)


def deserialize_into_node(target):
    return base("deserialize_into_node", target=target)


def retry(max_num_retries, *, inherited=False, user_display=True):
    """
    Retry the given node `max_num_retries` number of times.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base(
        "retry",
        max_num_retries=max_num_retries,
        inherited=inherited,
        user_display=user_display,
    )


def retry_then_skip(max_num_retries, *, inherited=False, user_display=True):
    """
    Retry the given node `max_num_retries` number of times. If it still hasn't
    succeeded, skip it.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base(
        "retry_then_skip",
        max_num_retries=max_num_retries,
        inherited=inherited,
        user_display=user_display,
    )


def retry_with_double_mem(max_num_retries, *, inherited=False, user_display=True):
    """
    Retry the given node `max_num_retries` number of times, doubling the memory
    estimate each time. The memory request will be capped at the limits
    permitted by the underlying service.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base(
        "retry_with_double_mem",
        max_num_retries=max_num_retries,
        inherited=inherited,
        user_display=user_display,
    )


def skip_some_errors(max_num_errors, *, inherited=False, user_display=True):
    """
    If the node fails with <= `max_num_errors` descendants that have errored, it
    will skip those nodes and reset them, thus passing over them.
    :param max_num_errors: Max errors to skip + reset.
    :return:
    """
    return base(
        "skip_some_errors",
        max_num_errors=max_num_errors,
        inherited=inherited,
        user_display=user_display,
    )


def email(to=None, cc=None, *, inherited=False, user_display=True):
    """
    When the node finishes, send a summary email to the specified recipients.
    :param to: Recipients in the "To" field
    :param cc: Recipients in the "CC" field
    :return:
    """
    return base("email", to=to, cc=cc, inherited=inherited, user_display=user_display)


def slack_status(
    recipient, message=None, node_summary=False, *, inherited=False, user_display=True
):
    """
    Post a summary of the node in the specified slack channel via the Slack integration.
    :param recipient: where to send the updates to, channel id or user id
    :param message: message to print, optional
    :param node_summary: print node summary, defaults to True if no message, False otherwise
    :return:
    """

    return base(
        "slack_status",
        recipient=recipient,
        message=message,
        node_summary=node_summary,
        inherited=inherited,
        user_display=user_display,
    )


def github_status(url, sha, *, inherited=False, user_display=True):
    return base(
        "github_status",
        url=url,
        sha=sha,
        inherited=inherited,
        user_display=user_display,
    )


def github_pipeline_status(url, sha, *, inherited=False, user_display=True):
    """
    reports Queued/Pending/Running/Done/Error counts for entire pipeline (root)
    regardless of node the event is attached to. However, setting `inherited=True`
    increases the sensitivity of updates to include the subtree.
    """
    return base(
        "github_pipeline_status",
        url=url,
        sha=sha,
        inherited=inherited,
        user_display=user_display,
    )


def handle_memory_errors(
    mem_limits=(8, 32), max_duration=3600, *, inherited=False, user_display=True
):
    """
    Logic:
     * If not a memoryError, abort.
     * Don't retry if memory would be above upper bound of `mem_limit`.
     * Don't retry if duration has already exceeded `max_duration`.
     * Retry if memory is below lower bound of `mem_limit`.
     * Retry if memory is above lower bound of `mem_limit` and we haven't retried it yet
       (try at least once).
    """
    return base(
        "handle_memory_errors",
        mem_limits=mem_limits,
        max_duration=max_duration,
        inherited=inherited,
        user_display=user_display,
    )
