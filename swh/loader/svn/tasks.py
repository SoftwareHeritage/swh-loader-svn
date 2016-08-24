# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.core import hashutil
from swh.loader.core import tasks

from .loader import SWHSvnLoader


class LoadSWHSvnRepositoryTsk(tasks.LoaderCoreTask):
    """Import one svn repository to Software Heritage.

    """
    CONFIG_BASE_FILENAME = 'loader/svn.ini'

    ADDITIONAL_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),
    }

    task_queue = 'swh_loader_svn'

    def run(self, *args, **kwargs):
        """Import a svn repository with swh policy.

        Args:
            args: ordered arguments (expected None)
            kwargs: Dictionary with the following expected keys:
              - svn_url: (mandatory) svn's repository url
              - destination_path: (mandatory) root directory to
                locally retrieve svn's data
              - swh_revision: (optional) extra SWH revision hex to
                start from.  cf. swh.loader.svn.SvnLoader.process
                docstring

        """
        destination_path = kwargs['destination_path']
        # local svn url
        svn_url = kwargs['svn_url']

        if 'origin' not in kwargs:  # first time, we'll create the origin
            origin = {
                'url': svn_url,
                'type': 'svn',
            }
            origin['id'] = self.storage.origin_add_one(origin)
        else:
            origin = {
                'id': kwargs['origin'],
                'url': svn_url,
                'type': 'svn'
            }

        date_visit = datetime.datetime.now(tz=datetime.timezone.utc)
        origin_visit = self.storage.origin_visit_add(origin['id'],
                                                     date_visit)

        origin_visit.update({
            'date': date_visit
        })

        # the real production use case with storage and all
        loader = SWHSvnLoader(svn_url, destination_path, origin)

        if 'swh_revision' in kwargs:
            swh_revision = hashutil.hex_to_hash(kwargs['swh_revision'])
        else:
            swh_revision = None

        result = loader.load(origin_visit, swh_revision)

        # Check for partial completion to complete state data
        if 'completion' in result and result['completion'] == 'partial':
            state = result['state']
            state.update({
                'destination_path': destination_path,
                'svn_url': svn_url,
            })
            result['state'] = state

        return result
