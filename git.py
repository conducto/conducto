import os
import conducto as co
from conducto.integrations import git

# from conducto import callback


def apply_status(parent, *, inherited=False):
    """initialize github checks status updating for node or subtree of nodes"""
    from conducto.shared import log

    url = os.environ.get("CONDUCTO_GIT_URL")
    sha = os.environ.get("CONDUCTO_GIT_SHA")
    if url and sha:
        root = parent
        git.add_status_callback(root, url, sha, inherited=True, user_display=False)
