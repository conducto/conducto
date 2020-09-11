from .. import api
import conducto as co
from conducto import callback
from conducto.shared import types as t, request_utils
from ..api import api_utils
import subprocess
import enum
import os


_log_diff_dict = {}


class LogType(enum.Enum):
    RAW = 1
    MARKDOWN = 2
    SLACK = 3


############################################################
# public methods
############################################################
def url(repo: str, token: t.Token = None) -> str:
    if t.Bool(os.getenv("CONDUCTO_GITHUB_USE_SECRETS", "1")):
        secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
        user = secrets["GITHUB_USER"]
        token = secrets["GITHUB_TOKEN"]
        owner = secrets["GITHUB_OWNER"]
        return f"https://{user}:{token}@github.com/{owner}/{repo}.git"
    else:
        u = api.Config().get_url()
        headers = api_utils.get_auth_headers(token=token)
        response = request_utils.get(
            f"{u}/integrations/github/url/{repo}", headers=headers
        )
        return api_utils.get_data(response)


def add_check_callbacks(node: co.Node, repo=None, sha=None):
    if repo is None:
        repo = os.environ["CONDUCTO_GIT_REPO"]
    if sha is None:
        sha = os.environ["CONDUCTO_GIT_SHA"]
    cb = callback.github_check(repo, sha)
    node.on_state_change(cb)


def commit_status(
    repo: str,
    sha: str,
    state,
    description=None,
    context=None,
    target_url=None,
    token: t.Token = None,
):
    data = {
        "state": state,
        "context": context,
        "description": description,
        "target_url": target_url,
    }
    secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
    access_token = secrets["GITHUB_TOKEN"]
    owner = secrets["GITHUB_OWNER"]

    url = f"https://api.github.com/repos/{owner}/{repo}/statuses/{sha}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = request_utils.post(url, headers=headers, data=data)
    api_utils.get_data(response)


def get_log_diff(
    base_branch: str,
    current_branch: str = None,
    log_type: LogType = LogType.RAW,
    max_lines=None,
) -> str:
    key = (base_branch, current_branch)
    log_diff = _log_diff_dict.get(key)
    if log_diff is None:
        log_diff = _get_raw_log_diff(base_branch, current_branch)
    if log_diff == "":
        return ""

    if log_type == LogType.RAW:
        return log_diff.replace("|", " ")

    lines = log_diff.split("\n")
    formatted_lines = []
    for line in lines:
        parts = line.split("|")
        name = parts[2].replace("[", "\[").replace("]", "\]")
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
def _get_raw_log_diff(base_branch, current_branch):
    log_cmd = ""
    if "CONDUCTO_PIPELINE_ID" in os.environ:
        log_cmd = (
            'git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*" && '
        )

    log_cmd += (
        f"git fetch origin {base_branch} && git log --abbrev-commit --date=relative "
        f"--pretty=format:'%h|%ad|[%an]|%d|%s' origin/{base_branch}..origin/{current_branch}"
    )
    result = subprocess.run(
        log_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
    )
    log_diff = result.stdout.decode()
    key = (base_branch, current_branch)
    _log_diff_dict[key] = log_diff
    return log_diff
