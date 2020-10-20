import os


def is_windows():
    # this method wraps host_detection since it is not packaged into all of our
    # docker containers
    try:
        import conducto.internal.host_detection as hostdet

        return hostdet.is_windows()
    except ImportError:
        return False


def makedirs(path, mode=0o777, exist_ok=False):
    """
    Clone of os.makedirs but with owner management.
    """
    parents = []
    current = path
    while True:
        if os.path.exists(current):
            break
        parents.append(current)
        current = os.path.dirname(current)

    # TODO:  If this fails part way through throwing an exception and some
    # parents have been created they will not receive the chown post
    # processing.
    os.makedirs(path, mode, exist_ok)

    if is_windows():
        return

    do_set, uid, gid = outer_set()
    if do_set:
        for parent in reversed(parents):
            os.chown(parent, uid, gid)


def outer_set():
    if os.getuid() != 0:
        # not root, let well enough alone
        return False, None, None
    outer = os.getenv("CONDUCTO_OUTER_OWNER")
    if not outer:
        # Possibly windows file system outside so this logic does not apply.
        return False, None, None
    uid, gid = outer.split(":")
    uid, gid = int(uid), int(gid)
    return True, uid, gid


def outer_chown(path):
    if is_windows():
        return
    do_set, uid, gid = outer_set()
    if do_set:
        os.chown(path, uid, gid)


def outer_chown_recursive(path):
    for dirpath, _dirnames, filenames in os.walk(path):
        outer_chown(dirpath)
        for filename in filenames:
            outer_chown(os.path.join(dirpath, filename))
