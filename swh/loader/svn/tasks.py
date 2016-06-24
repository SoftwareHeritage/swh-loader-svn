# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.core import tasks
from swh.loader.svn.loader import GitSvnSvnLoader, SWHSvnLoader


class LoadSvnRepositoryTsk(tasks.LoaderCoreTask):
    """Import one svn repository to Software Heritage.

    """
    CONFIG_BASE_FILENAME = 'loader/svn.ini'

    ADDITIONAL_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),
        'with_policy': ('string', 'swh'),  # Default, other possible
                                           # value is 'gitsvn'
    }

    task_queue = 'swh_loader_svn'

    def run(self, svn_url, local_path):
        """Import a svn repository.

        Args:
            cf. swh.loader.svn.SvnLoader.process docstring

        """
        origin = {'type': 'svn', 'url': svn_url}
        origin['id'] = self.storage.origin_add_one(origin)

        fetch_history_id = self.open_fetch_history(origin['id'])

        # Determine which loader to trigger
        if self.config['with_policy'] == 'gitsvn':
            loader = GitSvnSvnLoader(svn_url, local_path, origin)
        elif self.config['with_policy'] == 'swh':
            loader = SWHSvnLoader(svn_url, local_path, origin)
        else:
            raise ValueError('Only gitsvn or swh policies are supported in'
                             '\'with_policy\' entry. '
                             'Please adapt your svn.ini file accordingly')

        result = loader.load()

        self.close_fetch_history(fetch_history_id, result)
