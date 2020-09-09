from . import pipeline


class base:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def to_literal(self):
        d = {}
        for k, v in self.kwargs.items():
            if isinstance(v, pipeline.Node):
                d.setdefault("__node_args__", []).append(k)
                d[k] = v
            else:
                d[k] = v
        return [self.name, d]


def retry(max_num_retries):
    """
    Retry the given node `max_num_retries` number of times.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base("retry", max_num_retries=max_num_retries)


def retry_then_skip(max_num_retries):
    """
    Retry the given node `max_num_retries` number of times. If it still hasn't
    succeeded, skip it.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base("retry_then_skip", max_num_retries=max_num_retries)


def retry_with_double_mem(max_num_retries):
    """
    Retry the given node `max_num_retries` number of times, doubling the memory
    estimate each time. The memory request will be capped at the limits
    permitted by the underlying service.
    :param max_num_retries: Max iterations.
    :return:
    """
    return base("retry_with_double_mem", max_num_retries=max_num_retries)


def skip_some_errors(max_num_errors):
    """
    If the node fails with <= `max_num_errors` descendants that have errored, it
    will skip those nodes and reset them, thus passing over them.
    :param max_num_errors: Max errors to skip + reset.
    :return:
    """
    return base("skip_some_errors", max_num_errors=max_num_errors)


def email(to=None, cc=None):
    """
    When the node finishes, send a summary email to the specified recipients.
    :param to: Recipients in the "To" field
    :param cc: Recipients in the "CC" field
    :return:
    """
    return base("email", to=to, cc=cc)


def slack(to, token, message=None, node_summary=False):
    """
    Post a summary of the node in the specified slack channel.
    :param to: where to send the updates to
    :param token: slack token. Obtain it by running /gettoken in your slack channel
    :param message: message to print, optional
    :param node_summary: print node summary, defaults to True if no message, False otherwise
    :return:
    """

    return base("slack", to=to, token=token, message=message, node_summary=node_summary)


def github_status_update(repo, state, sha, owner=None, access_token=None):
    return base(
        "github_status_update",
        repo=repo,
        state=state,
        sha=sha,
        owner=owner,
        access_token=access_token,
    )


def github_check(repo, sha):
    return base("github_check", repo=repo, sha=sha)
