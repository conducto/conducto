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


def slack(channel, token, doc=None):
    """
    Post a summary of the node in the specified Slack channel.
    :param channel: channel to post in
    :param token:  slack token
    :param doc: extra documentation to print in slack messages
    :return:
    """
    return base("slack", channel=channel, token=token, doc=doc)


def github_status_update(owner, repo, sha, access_token, state, **kwargs):
    return base(
        "github_status_update",
        owner=owner,
        sha=sha,
        repo=repo,
        access_token=access_token,
        state=state,
        **kwargs
    )


def github_status_creator(owner, repo, sha, access_token):
    import functools

    return functools.partial(github_status_update, owner, repo, sha, access_token)
