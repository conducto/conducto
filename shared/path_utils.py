import os


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
    do_set, uid, gid = outer_set()
    if do_set:
        os.chown(path, uid, gid)


def is_parent_subdir(parent, subdir):
    # This function is focused on recognizing named mounts, hence it is biased
    # towards the windows code path. This is because named mounts are always
    # stored in the format of the docker host.
    import conducto.internal.host_detection as hostdet

    if os.getenv("WINDOWS_HOST") or hostdet.is_windows() or hostdet.is_wsl():
        import ntpath

        return parent.rstrip("/\\") == ntpath.commonpath([parent, subdir])
    else:
        import posixpath

        return parent.rstrip("/") == posixpath.commonpath([parent, subdir])
