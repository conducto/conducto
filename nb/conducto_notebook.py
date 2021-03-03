"""
command line utility that seamlessly passes command line arguments
to a notebook and runs the notebook, effectively making the notebook
a CLI script experience.
"""

import sys
from os import environ as env, path, remove, walk, execv
from pathlib import Path
import re
from html.parser import HTMLParser
import pkg_resources
import subprocess
from pathlib import Path
import conducto as co
from typing import Dict, Optional
import nbformat
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import shlex

# create atomic write functions (SO https://stackoverflow.com/a/46407326/1390640)
try:
    # Posix based file locking (Linux, Ubuntu, MacOS, etc.)
    # only works reliably for writable files
    import os, fcntl

    def lock_file(f):
        if f.writable():
            fcntl.lockf(f, fcntl.LOCK_EX)

    def unlock_file(f):
        if f.writable():
            fcntl.lockf(f, fcntl.LOCK_UN)


except ModuleNotFoundError:
    # Windows
    import msvcrt, os

    def file_size(f):
        return os.path.getsize(os.path.realpath(f.name))

    def lock_file(f):
        msvcrt.locking(f.fileno(), msvcrt.LK_RLCK, file_size(f))

    def unlock_file(f):
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, file_size(f))


# atomic version of file open - must be used in with context
class atomic_open:
    def __init__(self, path, *args, **kwargs):
        self.file = open(path, *args, **kwargs)
        lock_file(self.file)

    def __enter__(self, *args, **kwargs):
        return self.file

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        self.file.flush()
        os.fsync(self.file.fileno())
        unlock_file(self.file)
        self.file.close()
        if exc_type != None:
            return False
        else:
            return True


def process_help_command(args):
    if len(args) > 1:
        cmd = args[1].lower()
        if cmd not in ("help", "-h", "--help"):
            return
    print(
        """

conducto-notebook <notebook_file.ipynb> <-p argument>...

Runs ipython notebooks from the command line, passing user arguments
using papermill format.
Either tag a cell with declaration of default variables with 'parameters'
or make the first line of that cell '# parameters' will allow overriding
those values from the command line.

Usage Example:
  conducto-notebook notebook.ipynb -p depth True -p year 1992 -p title "Early '90s Analysis"

Will override the following cell content defaults. Use '# parameters' or you can tag the cell.
# parameters
depth = False
year = 2000
title = "<plot title>"

"""
    )
    sys.exit(0)


def validate_args(args):
    if len(args) < 2:
        print(f"Error: not enough arguments. got {len(args)}")
        sys.exit(1)
    filename = args[1]
    if not path.isfile(filename):
        print(f"Error: not a file '{filename}'")
        sys.exit(1)


# papermill example:
# $ papermill {filename} {filename} -p data_set 5 -p use_dates True --request-save-on-cell-execute --autosave-cell-every 5 &
RUN_PAPERMILL_IN_BACKGROUND = """
papermill {filename} {filename} {params} --request-save-on-cell-execute --autosave-cell-every 5 &
"""

RUN_PAPERMILL_INJECTION = """
papermill {filename} {filename} {params} --prepare-only
"""


class ConductoNotebook:
    """reading and writing operations for notebooks, aside from papermill
    main funcs:
      preprocess(cell): convert '# parameters' syntax to 'parameters' papermill cell tag. This is support for non-tag editors like VSCode
      postprocess(): remove papermill's vague comment, which also conflicts with our syntax, replace with more informative one
      save(optional_alternate_filname): writes notebook file to disk
    helpers:
      _has_tag(cell,tag) -> bool: detects papermill's 'parameters' tag
      _has_params_comment(cell) -> bool: detects '# parameters' syntax
      _remove_params_comment(cell) -> cell: removes papermill's default "# Parameters" vague comment
      refresh_from_disk(): re-read notebook file from disk"""

    def __init__(self, notebook_filename, **kw):
        self.notebook_filename = notebook_filename
        self.refresh_from_disk()

    def _has_tag(self, cell, tag) -> bool:
        if "tags" in cell.metadata and tag in cell.metadata.tags:
            return True
        return False

    def _has_params_comment(self, cell) -> bool:
        """check first line of cell for '#\s*parameters' regex pattern"""
        for line in cell.source.split("\n"):
            line = line.strip()
            if re.search(r"#\s*parameters", line.lower()):
                return True
            break
        return False

    def _remove_params_comment(self, cell):
        """remove unclear top-line params comment added by papermill"""
        cell.source = "\n".join(cell.source.split("\n")[1:]).strip()
        return cell

    def refresh_from_disk(self):
        """re-read the file"""
        unread = True
        attempts = 0
        patience = 50
        while unread and attempts <= patience:
            try:
                self.contents = nbformat.read(self.notebook_filename, as_version=4)
                unread = not unread
            except:
                attempts += 1
                time.sleep(0.01)
        if attempts > patience:
            print(
                f"Error: could not read notebook {self.notebook_filename}\nEnsure correctly formatted JSON."
            )

    def preprocess(self):
        """add support for '#parameters' in lieu of messing with tags
        this converts '#parameters' to a tag."""

        for cell_i, cell in enumerate(self.contents.cells):
            if self._has_params_comment(cell):
                tags = self.contents.cells[cell_i].metadata.get("tags", list())
                if "parameters" not in tags:
                    tags.append("parameters")
                self.contents.cells[cell_i].metadata["tags"] = tags

    def papermill_prepare(self, params):
        # only inject params (used to prepare for notebook debug/edit)
        subprocess.run(
            RUN_PAPERMILL_INJECTION.format(
                filename=self.notebook_filename, params=params
            ),
            shell=True,
            stdout=subprocess.DEVNULL,
        )

    def papermill_execute_in_background(self, params):
        # inject and run
        subprocess.run(
            RUN_PAPERMILL_IN_BACKGROUND.format(
                filename=self.notebook_filename, params=params
            ),
            shell=True,
            stdout=subprocess.DEVNULL,
        )

    def postprocess(self):
        """remove '#parameters' comment in injected cell from papermill
        as it would cause recursion on next run
        and replace with more informative comment"""

        for cell_i, cell in enumerate(self.contents.cells):
            if self._has_tag(cell, "injected-parameters"):
                if self._has_params_comment(cell):
                    self.contents.cells[cell_i] = self._remove_params_comment(cell)
                    self.contents.cells[cell_i].source = (
                        "# Command Line Parameters injected from papermill\n"
                        + self.contents.cells[cell_i].source
                    )

    def save(self, out_filename=None):
        out_filename = self.notebook_filename if not out_filename else out_filename
        with atomic_open(out_filename, "w+t") as out_file:
            out_file.write(nbformat.writes(self.contents))


class WaitAtLeastFilter:
    """returns true if `seconds` have passed
    since the last successful call of returning True"""

    def __init__(self, seconds):
        self.delay = seconds
        self.begin_okay = self.delay

    def __call__(self) -> bool:
        if time.time() > self.begin_okay:
            self.begin_okay = time.time() + self.delay
            return True
        return False


class NotebookModifiedEventHandler(FileSystemEventHandler):
    def __init__(self, nb: ConductoNotebook, observer: Observer):
        self.nb = nb
        self.observer = observer
        self.ignore_events = False
        self.can_update_gui = WaitAtLeastFilter(1.0)  # seconds since prior GUI update
        self.running_as_conducto_node = (
            "CONDUCTO_EXECUTION_ENV" in os.environ
        )  # determins rendering to temp file + html
        super().__init__()

    def on_modified(self, event):
        if self.ignore_events:
            return

        if event.src_path:
            self.ignore_events = True
            self.nb.refresh_from_disk()
            finished = bool(self.nb.contents.metadata.papermill.end_time)
            if self.can_update_gui() and self.running_as_conducto_node:
                # save to non-papermill file for front-end rendering
                self.nb.postprocess()
                self.nb.save("/tmp/notebookrender.ipynb")
            if finished:
                self.observer.stop()
                time.sleep(0.25)
                self.nb.postprocess()
                self.nb.save()
            else:
                self.ignore_events = not self.ignore_events


def process_format_command(args):
    # ex: conducto-notebook format notebook.ipynb
    if len(args) < 3:
        return
    if args[1].lower() != "format" or not os.path.isfile(args[2]):
        return
    script_name = sys.argv[0]
    cmd = sys.argv[1]
    filename = sys.argv[2]
    co_nb_params = (
        " ".join([shlex.quote(arg) for arg in sys.argv[3:]])
        if len(sys.argv) > 3
        else ""
    )

    nb = ConductoNotebook(filename)
    nb.preprocess()
    nb.save()
    nb.papermill_prepare(co_nb_params)
    nb.refresh_from_disk()
    nb.postprocess()
    nb.save()
    sys.exit(0)


def main():
    process_help_command(sys.argv)
    process_format_command(sys.argv)
    validate_args(sys.argv)

    # perform main function of conducto-notebook - to run papermill
    script_name = sys.argv[0]
    filename = sys.argv[1]
    co_nb_params = (
        " ".join([shlex.quote(arg) for arg in sys.argv[2:]])
        if len(sys.argv) > 2
        else ""
    )

    nb = ConductoNotebook(filename)
    observer = Observer()
    notebook_modified_handler = NotebookModifiedEventHandler(nb, observer)

    nb.preprocess()
    nb.save()

    # begin watching notebook for changes to Render, and Reformat on finish
    observer.schedule(notebook_modified_handler, filename, recursive=False)
    observer.start()

    nb.papermill_execute_in_background(co_nb_params)

    # required for watchdog thread
    observer.join()


if __name__ == "__main__":
    main()
