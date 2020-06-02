# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from .loader import SvnLoader, SvnLoaderFromDumpArchive, SvnLoaderFromRemoteDump


@shared_task(name=__name__ + ".LoadSvnRepository")
def load_svn(
    *,
    url=None,
    origin_url=None,
    destination_path=None,
    swh_revision=None,
    visit_date=None,
    start_from_scratch=False,
):
    """Import a svn repository

    Args:
        args: ordered arguments (expected None)
        kwargs: Dictionary with the following expected keys:

          - url (str): (mandatory) svn's repository url
          - origin_url (str): Optional original url override
          - destination_path (str): (optional) root directory to
            locally retrieve svn's data
          - swh_revision (dict): (optional) extra revision hex to
            start from.  see swh.loader.svn.SvnLoader.process
            docstring

    """
    loader = SvnLoader(
        url,
        origin_url=origin_url,
        destination_path=destination_path,
        swh_revision=swh_revision,
        visit_date=visit_date,
        start_from_scratch=start_from_scratch,
    )
    return loader.load()


@shared_task(name=__name__ + ".MountAndLoadSvnRepository")
def load_svn_from_archive(
    *, url=None, archive_path=None, visit_date=None, start_from_scratch=False
):
    """1. Mount an svn dump from archive as a local svn repository
       2. Load it through the svn loader
       3. Clean up mounted svn repository archive

    """
    loader = SvnLoaderFromDumpArchive(
        url,
        archive_path=archive_path,
        visit_date=visit_date,
        start_from_scratch=start_from_scratch,
    )
    return loader.load()


@shared_task(name=__name__ + ".DumpMountAndLoadSvnRepository")
def load_svn_from_remote_dump(
    *, url=None, origin_url=None, visit_date=None, start_from_scratch=False
):
    """1. Mount a remote svn dump as a local svn repository.
       2. Load it through the svn loader.
       3. Clean up mounted svn repository archive.

    """
    loader = SvnLoaderFromRemoteDump(
        url,
        origin_url=origin_url,
        visit_date=visit_date,
        start_from_scratch=start_from_scratch,
    )
    return loader.load()
