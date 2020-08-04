from .. import api
from conducto.shared import types as t, request_utils
from . import api_utils
import subprocess
import enum
import os


class Git:
    def __init__(self):
        self.config = api.Config()
        self._url = self.config.get_url()
        self._log_diff_dict = {}

    ############################################################
    # public methods
    ############################################################
    def url(self, repo: str, token: t.Token = None) -> str:
        secrets = api.Secrets().get_user_secrets(token=token, include_org_secrets=True)
        github_user = secrets["GITHUB_USER"]
        github_token = secrets["GITHUB_TOKEN"]
        return f"https://{github_user}:{github_token}@github.com/conducto/{repo}.git"
        # headers = api_utils.get_auth_headers(token=token)
        # response = request_utils.get(
        #     self._url + f"/integrations/github/url/{repo}", headers=headers
        # )
        # return api_utils.get_text(response)

    class LogType(enum.Enum):
        RAW = 1
        MARKDOWN = 2
        SLACK = 3

    def get_log_diff(
        self,
        base_branch: str,
        current_branch: str = None,
        log_type: LogType = LogType.RAW,
        max_lines=None,
    ) -> str:
        key = (base_branch, current_branch)
        log_diff = self._log_diff_dict.get(key)
        if log_diff is None:
            log_diff = self._get_raw_log_diff(base_branch, current_branch)
        if log_diff == "":
            return ""

        if log_type == Git.LogType.RAW:
            return log_diff.replace("|", " ")

        lines = log_diff.split("\n")
        formatted_lines = []
        for line in lines:
            parts = line.split("|")
            name = parts[2].replace("[", "\[").replace("]", "\]")
            refs = f"_{parts[3]}_" if len(parts[3]) else ""
            if log_type == Git.LogType.MARKDOWN:
                formatted_line = (
                    f"* `{parts[0]}` {parts[1]}, **{name}** {refs}\n    - {parts[4]}"
                )
            elif log_type == Git.LogType.SLACK:
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

    def _get_raw_log_diff(self, base_branch, current_branch):
        log_cmd = ""
        if "CONDUCTO_PIPELINE_ID" in os.environ:
            log_cmd = 'git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*" && '

        log_cmd += (
            f"git fetch origin {base_branch} && git log --abbrev-commit --date=relative "
            f"--pretty=format:'%h|%ad|[%an]|%d|%s' origin/{base_branch}..origin/{current_branch}"
        )
        result = subprocess.run(
            log_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        log_diff = result.stdout.decode()
        key = (base_branch, current_branch)
        self._log_diff_dict[key] = log_diff
        return log_diff


AsyncGit = api_utils.async_helper(Git)
