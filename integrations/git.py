import enum
import os
import re
import urllib.parse

from .. import api
import conducto as co
from conducto import callback
from conducto.shared import client_utils, imagepath, request_utils, types as t
from ..api import api_utils


_log_diff_dict = {}


class LogType(enum.Enum):
    RAW = 1
    MARKDOWN = 2
    SLACK = 3


############################################################
# public methods
############################################################
def url(url: str, token: t.Token = None) -> str:
    if t.Bool(os.getenv("CONDUCTO_GITHUB_USE_SECRETS")):
        secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
        user = secrets.get("GITHUB_USER")
        token = secrets.get("GITHUB_TOKEN")
        if user and token:
            owner, repo = _parse_github_url(url)
            return f"https://{user}:{token}@github.com/{owner}/{repo}.git"
        else:
            return url
    else:
        query_url = f"{api.Config().get_url()}/integrations/github/url?"
        query_url += urllib.parse.urlencode({"url": url})
        headers = api_utils.get_auth_headers(token=token)
        response = request_utils.get(query_url, headers=headers)
        return api_utils.get_data(response)


def clone_url(url: str, token: t.Token = None) -> str:
    # Non-GitHub URLs should be passed through unchanged
    if not _is_github_url(url):
        return url

    # Custom override to use username/token from secrets
    if t.Bool(os.getenv("CONDUCTO_GITHUB_USE_SECRETS")):
        secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
        user = secrets.get("GITHUB_USER")
        gh_token = secrets.get("GITHUB_TOKEN")
        owner, repo = _parse_github_url(url)
        return f"https://{user}:{gh_token}@github.com/{owner}/{repo}.git"

    # Default action is to pass this through the GitHub integration, which will add the
    # appropriate auth and redirect to GitHub.
    if token is None:
        token = co.api.Config().get_token(refresh=True)
    query_url = f"{api.Config().get_url()}/integrations/github/clone_url/{token}/{url}"
    return query_url


def add_status_callback(node: co.Node, url, sha):
    cb = callback.github_status(url, sha)
    node.on_state_change(cb)


def get_log_diff(
    base_branch: str,
    current_branch: str = None,
    log_type: LogType = LogType.RAW,
    max_lines=None,
    git_dir=None,
) -> str:
    git_root = _find_git_root(git_dir)

    key = (base_branch, current_branch, git_root)
    log_diff = _log_diff_dict.get(key)
    if log_diff is None:
        log_diff = _get_raw_log_diff(base_branch, current_branch, git_root)
        _log_diff_dict[key] = log_diff
    if log_diff == "":
        return ""

    if log_type == LogType.RAW:
        return log_diff.replace("|", " ")

    lines = log_diff.split("\n")
    formatted_lines = []
    for line in lines:
        parts = line.split("|")
        name = parts[2].replace("[", "\\[").replace("]", "\\]")
        refs = f"_{parts[3]}_" if len(parts[3]) else ""
        if log_type == LogType.MARKDOWN:
            formatted_line = (
                f"* `{parts[0]}` {parts[1]}, **{name}** {refs}\n    - {parts[4]}"
            )
        elif log_type == LogType.SLACK:
            formatted_line = (
                f"• `{parts[0]}` {parts[1]}, *{parts[2]}* {refs} - {parts[4]}"
            )
        else:
            raise Exception(f"log diff format {log_type} not implemented")
        formatted_lines.append(formatted_line)

    if max_lines is None:
        max_lines = len(formatted_lines)
    num_extra = len(formatted_lines) - max_lines
    if max_lines is not None and num_extra > 0:
        formatted_lines = formatted_lines[:max_lines]
        formatted_lines.append(f"and {num_extra} more...")

    return "\n\n".join(formatted_lines)


############################################################
# internal helpers
############################################################
def _get_raw_log_diff(base_branch, current_branch, git_root):
    if "CONDUCTO_GIT_URL" in os.environ:
        client_utils.subprocess_run(
            [
                "git",
                "-C",
                git_root,
                "config",
                "remote.origin.fetch",
                "+refs/heads/*:refs/remotes/origin/*",
            ]
        )
        client_utils.subprocess_run(["git", "fetch", "origin", base_branch])

    result = client_utils.subprocess_run(
        [
            "git",
            "-C",
            git_root,
            "log",
            "--abbrev-commit",
            "--date=relative",
            "--pretty=format:%h|%ad|[%an]|%d|%s",
            f"origin/{base_branch}...origin/{current_branch}",
        ],
        capture_output=True,
    )
    return result.stdout.decode()


def _is_github_url(url):
    return "github.com" in url


def _parse_github_url(url):
    """
    Handle URLs of the form:
     - git://github.com/conducto/super.git
     - git@github.com:conducto/super.git
     - https://github.com/conducto/super.git
     - git@github.com-user/conducto/super.git
    Look for "github.com/{owner}/{repo}.git" or "github.com:{owner}/{repo}.git" or "github.com-user:{owner}/{repo}.git"
    """
    m = re.search(r"github\.com(?:-.*?)?[/:]([^/]+)/(.+?)\.git$", url)
    owner, repo = m.group(1, 2)
    return owner, repo


def _find_git_root(p):
    if p is None:
        p = co.image._non_conducto_dir()
    if not os.path.isabs(p):
        p = os.path.join(co.image._non_conducto_dir(), p)
    p = os.path.realpath(p)
    return imagepath.Path._get_git_root(p)
