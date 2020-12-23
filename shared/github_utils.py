from conducto.shared import constants

# useful utils in both private and public

# strings for github status states
class State:
    ERROR = "error"
    SUCCESS = "success"
    FAILURE = "failure"
    PENDING = "pending"


# strings for github status contexts
# (the bold part of the status)
# Loading is for status of pipeline until launch success
# Action is for status of entire pipeline
# and the viable action is figured out.
class Context:
    Loading = "Conducto Initialization"


def translate_event_for_UI(event: str) -> str:
    event = event.lower()
    result = ""
    CFG = constants.CONFIG_EVENTS
    if event == CFG.PR:
        result = "pull request"
    elif event == CFG.CREATE_BRANCH:
        result = "create branch"
    elif event == CFG.DELETE_BRANCH:
        result = "delete branch"
    elif event == CFG.CREATE_TAG:
        result = "create tag"
    elif event == CFG.DELETE_TAG:
        result = "delete tag"
    else:
        result = event
    # capitalize with title case
    return result.title()
