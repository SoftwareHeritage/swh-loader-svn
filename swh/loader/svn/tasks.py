# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import current_app as app

from .loader import (
    SvnLoader, SvnLoaderFromDumpArchive, SvnLoaderFromRemoteDump
)


@app.task(name=__name__ + '.LoadSvnRepository')
def load_svn(svn_url,
             destination_path=None,
             swh_revision=None,
             origin_url=None,
             visit_date=None,
             start_from_scratch=None):
    """Import a svn repository

    Args:
        args: ordered arguments (expected None)
        kwargs: Dictionary with the following expected keys:

          - svn_url (str): (mandatory) svn's repository url
          - destination_path (str): (mandatory) root directory to
            locally retrieve svn's data
          - origin_url (str): Optional original url override
          - swh_revision (dict): (optional) extra revision hex to
            start from.  see swh.loader.svn.SvnLoader.process
            docstring

    """
    return SvnLoader().load(
        svn_url=svn_url,
        destination_path=destination_path,
        origin_url=origin_url,
        swh_revision=swh_revision,
        visit_date=visit_date,
        start_from_scratch=start_from_scratch)


@app.task(name=__name__ + '.MountAndLoadSvnRepository')
def mount_load_svn(archive_path, origin_url=None, visit_date=None,
                   start_from_scratch=False):
    """1. Mount an svn dump from archive as a local svn repository
       2. Load it through the svn loader
       3. Clean up mounted svn repository archive

    """
    return SvnLoaderFromDumpArchive(archive_path).load(
        svn_url=None,
        origin_url=origin_url,
        visit_date=visit_date,
        archive_path=archive_path,
        start_from_scratch=start_from_scratch)


@app.task(name=__name__ + '.DumpMountAndLoadSvnRepository')
def dump_mount_load_svn(svn_url, origin_url=None, visit_date=None,
                        start_from_scratch=False):
    """1. Mount an svn dump from archive as a local svn repository.
       2. Load it through the svn loader.
       3. Clean up mounted svn repository archive.

    """
    return SvnLoaderFromRemoteDump().load(
        svn_url=svn_url,
        origin_url=origin_url,
        visit_date=visit_date,
        start_from_scratch=start_from_scratch)
