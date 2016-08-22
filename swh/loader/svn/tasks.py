# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.core import hashutil
from swh.loader.core import tasks

from .loader import GitSvnSvnLoader, SWHSvnLoader


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

    def run(self, *args, **kwargs):
        """Import a svn repository.

        Args:
            - svn_url: svn's repository url
            - destination_path: root directory to locally retrieve svn's data
            - swh_revision: Optional extra swh revision to start from.
            cf. swh.loader.svn.SvnLoader.process docstring

        """
        destination_path = kwargs['destination_path']
        # local svn url
        svn_url = kwargs['svn_url']
        # if original_svn_url is mentioned, this means we load a local mirror
        original_svn_url = kwargs.get('original_svn_url')
        # potential uuid overwrite
        original_svn_uuid = kwargs.get('original_svn_uuid')

        # Make sure we have all that's needed
        if original_svn_url and not original_svn_uuid:
            msg = "When loading a local mirror, you must specify the original repository's uuid."  # noqa
            self.log.error('%s. Skipping mirror %s' % (msg, svn_url))
            return

        # Determine the origin url
        origin_url = original_svn_url if original_svn_url else svn_url

        if 'origin' not in kwargs:  # first time, we'll create the origin
            origin = {
                'url': origin_url,
                'type': 'svn',
            }
            origin['id'] = self.storage.origin_add_one(origin)
        else:
            origin = {
                'id': kwargs['origin'],
                'url': origin_url,
                'type': 'svn'
            }

        date_visit = datetime.datetime.now(tz=datetime.timezone.utc)
        origin_visit = self.storage.origin_visit_add(origin['id'],
                                                     date_visit)

        origin_visit.update({
            'date': date_visit
        })

        # Determine which loader to trigger
        if self.config['with_policy'] == 'gitsvn':
            # this one compute hashes but do not store anywhere
            loader = GitSvnSvnLoader(svn_url, destination_path, origin,
                                     svn_uuid=original_svn_uuid)
        elif self.config['with_policy'] == 'swh':
            # the real production use case with storage and all
            loader = SWHSvnLoader(svn_url, destination_path, origin,
                                  svn_uuid=original_svn_uuid)
        else:
            raise ValueError('Only gitsvn or swh policies are supported in'
                             '\'with_policy\' entry. '
                             'Please adapt your svn.ini file accordingly')
        if 'swh_revision' in kwargs:
            swh_revision = hashutil.hex_to_hash(kwargs['swh_revision'])
        else:
            swh_revision = None

        result = loader.load(origin_visit, swh_revision)

        # Check for partial completion to complete state data
        if 'completion' in result and result['completion'] == 'partial':
            state = result['state']
            state.update({
                'svn_url': svn_url,
                'original_svn_url': origin_url,
                'original_svn_uuid': original_svn_uuid,
            })
            result['state'] = state

        return result
