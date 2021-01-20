"""
This is a fully functional do nothing backend to provide a template to backend
writers.  It is fully functional in that you can select it as a backend e.g.
with ::

    import matplotlib
    matplotlib.use("template")

and your program will (should!) run without error, though no output is
produced.  This provides a starting point for backend writers; you can
selectively implement drawing methods (`draw_path`, `draw_image`, etc.) and
slowly see your figure come to life instead having to have a full blown
implementation before getting any results.

Copy this file to a directory outside of the Matplotlib source tree, somewhere
where Python can import it (by adding the directory to your ``sys.path`` or by
packaging it as a normal Python package); if the backend is importable as
``import my.backend`` you can then select it using ::

    import matplotlib
    matplotlib.use("module://my.backend")

If your backend implements support for saving figures (i.e. has a `print_xyz`
method), you can register it as the default handler for a given file type::

    from matplotlib.backend_bases import register_backend
    register_backend('xyz', 'my_backend', 'XYZ File Format')
    ...
    plt.savefig("figure.xyz")
"""

import conducto as co
import os
import uuid
import weakref

from matplotlib._pylab_helpers import Gcf
from matplotlib.backends.backend_agg import FigureManagerBase, FigureCanvasAgg
from matplotlib.figure import Figure


########################################################################
#
# The following functions and classes are for pyplot and implement
# window/figure managers, etc...
#
########################################################################

# Gcf.get_all_fig_managers() returns all FigureManagers used so far. Keep a record so
# we don't print plots multiple times.
_SHOWN_MANAGERS = weakref.WeakSet()


def show(*, block=None):
    """
    For image backends - is not required.
    For GUI backends - show() is usually the last line of a pyplot script and
    tells the backend that it is time to draw.  In interactive mode, this
    should do nothing.
    """
    if not os.path.exists("/conducto/data/pipeline"):
        raise EnvironmentError(
            "Cannot find /conducto/data/pipeline. Conducto backend for matplotlib must "
            "be used inside the Conducto environment."
        )

    if not co.nb.IN_MARKDOWN:
        print("\n<ConductoMarkdown>")
    for manager in Gcf.get_all_fig_managers():
        if manager in _SHOWN_MANAGERS:
            continue

        basepath = f".mpl_plots/{uuid.uuid4()}.png"
        fullpath = f"/conducto/data/pipeline/{basepath}"
        os.makedirs(os.path.dirname(fullpath), exist_ok=True)

        manager.canvas.figure.savefig(fullpath)

        url = co.data.pipeline.url(basepath)
        print(f"![plot]({url})")

        _SHOWN_MANAGERS.add(manager)

    if not co.nb.IN_MARKDOWN:
        print("</ConductoMarkdown>")


def new_figure_manager(num, *args, FigureClass=Figure, **kwargs):
    """Create a new figure manager instance."""
    # If a main-level app must be created, this (and
    # new_figure_manager_given_figure) is the usual place to do it -- see
    # backend_wx, backend_wxagg and backend_tkagg for examples.  Not all GUIs
    # require explicit instantiation of a main-level app (e.g., backend_gtk3)
    # for pylab.
    thisFig = FigureClass(*args, **kwargs)
    return new_figure_manager_given_figure(num, thisFig)


def new_figure_manager_given_figure(num, figure):
    """Create a new figure manager instance for the given figure."""
    canvas = FigureCanvasAgg(figure)
    manager = FigureManagerBase(canvas, num)
    return manager


########################################################################
#
# Now just provide the standard names that backend.__init__ is expecting
#
########################################################################

FigureCanvas = FigureCanvasAgg
FigureManager = FigureManagerBase
