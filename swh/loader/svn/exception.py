# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class SvnLoaderEventful(ValueError):
    """Loading happens with some events. This transit the latest revision
    seen.

    """

    def __init__(self, e, swh_revision):
        super().__init__(e)
        self.swh_revision = swh_revision


class SvnLoaderUneventful(ValueError):
    """'Loading did nothing."""

    pass


class SvnLoaderHistoryAltered(ValueError):
    """History altered detected"""

    pass
