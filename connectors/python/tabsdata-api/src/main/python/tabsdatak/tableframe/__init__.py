"""TableFrame placeholder.

A tabsdata flow exchanges tabular batches between functions as
:class:`TableFrame` instances. The concrete implementation lives
outside this package; this stub keeps the API surface annotatable
without pulling the heavyweight tableframe runtime into
``tabsdata-api``.
"""


class TableFrame:
    """Opaque batch of rows exchanged across flow boundaries.

    Decorated functions accept it as input (and emit it as output)
    via :data:`tabsdatak.api.TableFrameSpec` /
    :data:`tabsdatak.api.TableFramesSpec`. The runtime owns
    materialisation; user code treats instances as opaque values.
    """

    pass
