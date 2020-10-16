import os
import conducto as co
from conducto.integrations import git


def apply_status_all(parent):
    url = os.environ.get("CONDUCTO_GIT_URL")
    sha = os.environ.get("CONDUCTO_GIT_SHA")
    if url and sha:
        for node in parent.stream():
            if isinstance(node, co.Exec):
                git.add_status_callback(node, url, sha)
