import enum
import os
import re
import urllib.parse

from .. import api
import conducto as co
from conducto import callback
from conducto.shared import client_utils, imagepath, request_utils, types as t
from ..api import api_utils


_log_diff_cache = {}


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
    if _is_github_url(url):
        return _github_clone_url(url, token)
    elif _is_bitbucket_url(url):
        return _bitbucket_clone_url(url, token)
    else:
        # Non-GitHub URLs should be passed through unchanged
        return url


def _github_clone_url(url: str, token: t.Token = None) -> str:
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


def _bitbucket_clone_url(url: str, token: t.Token = None) -> str:
    # Custom override to use username/token from secrets
    if t.Bool(os.getenv("CONDUCTO_BITBUCKET_USE_SECRETS")):
        secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
        user = secrets.get("BITBUCKET_USER")
        gh_token = secrets.get("BITBUCKET_TOKEN")
        owner, repo = _parse_bitbucket_url(url)
        return f"https://{user}:{gh_token}@bitbucket.org/{owner}/{repo}.git"

    # Default action is to pass this through the GitHub integration, which will add the
    # appropriate auth and redirect to GitHub.
    if token is None:
        token = co.api.Config().get_token(refresh=True)
    query_url = (
        f"{api.Config().get_url()}/integrations/bitbucket/clone_url/{token}/{url}"
    )
    return query_url


def add_status_callback(
    node: co.Node, url: str, sha: str, *, inherited=False, user_display=True
):
    from conducto.shared import log

    """Enable automatic Github Checks status updating for a node or nodes; and toggle their display in the Callbacks listing in the UI"""
    cb = callback.github_status(
        url, sha, inherited=inherited, user_display=user_display
    )
    node.on_state_change(cb)


def get_log_diff(
    base_branch: str,
    current_branch: str = None,
    log_type: LogType = LogType.RAW,
    max_lines=None,
    git_dir=None,
) -> str:
    raw_diff = _get_log_diff_for_key("raw_diff", base_branch, current_branch, git_dir)

    if raw_diff == "":
        return ""

    if log_type == LogType.RAW:
        return raw_diff.replace("|", " ")

    lines = raw_diff.split("\n")
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


def get_num_commits(base_branch: str, current_branch: str = None, git_dir=None) -> int:
    return _get_log_diff_for_key("num_commits", base_branch, current_branch, git_dir)


def get_num_commits_behind_base(
    base_branch: str, current_branch: str = None, git_dir=None
) -> int:
    return _get_log_diff_for_key(
        "num_commits_behind_base", base_branch, current_branch, git_dir
    )


def get_last_shared_commit(
    base_branch: str, current_branch: str = None, git_dir=None
) -> str:
    return _get_log_diff_for_key(
        "last_shared_commit", base_branch, current_branch, git_dir
    )


############################################################
# internal helpers
############################################################
def _get_log_diff_for_key(
    diff_key: str, base_branch: str, current_branch: str = None, git_dir=None
) -> str:
    git_root = _find_git_root(git_dir)

    key = (base_branch, current_branch, git_root)
    log_diff = _log_diff_cache.get(key)
    if log_diff is None:
        log_diff = _get_raw_log_diff(base_branch, current_branch, git_root)
        _log_diff_cache[key] = log_diff
    return log_diff[diff_key]


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
    num_commits_including_behind = len(result.stdout.decode().split("\n"))

    result = client_utils.subprocess_run(
        [
            "git",
            "-C",
            git_root,
            "log",
            "--abbrev-commit",
            "--date=relative",
            "--pretty=format:%h|%ad|[%an]|%d|%s",
            f"origin/{base_branch}..origin/{current_branch}",
        ],
        capture_output=True,
    )
    raw_log_diff = result.stdout.decode()
    num_commits = len(raw_log_diff.split("\n"))

    num_commits_behind_base = num_commits_including_behind - num_commits
    result = client_utils.subprocess_run(
        [
            "git",
            "-C",
            git_root,
            "merge-base",
            f"origin/{base_branch}",
            f"origin/{current_branch}",
        ],
        capture_output=True,
    )
    last_shared_commit = result.stdout.decode()[:8]

    return {
        "raw_diff": raw_log_diff,
        "num_commits": num_commits,
        "num_commits_behind_base": num_commits_behind_base,
        "last_shared_commit": last_shared_commit,
    }


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
    if not m:
        raise ValueError(f"{url} is not a valid github url.")

    owner, repo = m.group(1, 2)
    return owner, repo


def _is_bitbucket_url(url):
    return "bitbucket.org" in url


def _parse_bitbucket_url(url):
    """
    Handle URLs of the form:
     - git://bitbucket.org/conducto/super.git
     - git@bitbucket.org:conducto/super.git
     - https://bitbucket.org/conducto/super.git
     - git@bitbucket.org-user/conducto/super.git
    Look for "bitbucket.org/{owner}/{repo}.git" or "bitbucket.com:{owner}/{repo}.git" or "bitbucket.com-user:{owner}/{repo}.git"
    """
    m = re.search(r"bitbucket\.org(?:-.*?)?[/:]([^/]+)/(.+?)\.git$", url)
    if not m:
        raise ValueError(f"{url} is not a valid bitbucket url.")

    owner, repo = m.group(1, 2)
    return owner, repo


def _find_git_root(p):
    if p is None:
        p = co.image._non_conducto_dir()
    if not os.path.isabs(p):
        p = os.path.join(co.image._non_conducto_dir(), p)
    p = os.path.realpath(p)
    return imagepath.Path._get_git_root(p)
