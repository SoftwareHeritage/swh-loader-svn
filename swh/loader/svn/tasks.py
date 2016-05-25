# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader import tasks
from swh.loader.svn.loader import SvnLoader


class LoadSvnRepositoryTsk(tasks.LoaderCoreTask):
    """Import a svn repository to Software Heritage

    """
    task_queue = 'swh_loader_svn'

    def run(self, svn_url, local_path):
        """Import a svn repository.

        Args:
            cf. swh.loader.svn.SvnLoader.process docstring

        """
        storage = SvnLoader().storage

        origin = {'type': 'svn', 'url': svn_url}
        origin['id'] = storage.origin_add_one(origin)

        fetch_history_id = self.open_fetch_history(storage, origin['id'])

        result = SvnLoader(origin['id']).process(svn_url, origin, local_path)

        self.close_fetch_history(storage, fetch_history_id, result)
