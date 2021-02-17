# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Optional

from celery import shared_task
import iso8601

from .loader import SvnLoader, SvnLoaderFromDumpArchive, SvnLoaderFromRemoteDump


def convert_to_datetime(date: Optional[str]) -> Optional[datetime]:
    try:
        return iso8601.parse_date(date)
    except Exception:
        return None


@shared_task(name=__name__ + ".LoadSvnRepository")
def load_svn(
    *,
    url: Optional[str] = None,
    origin_url: Optional[str] = None,
    destination_path: Optional[str] = None,
    swh_revision: Optional[str] = None,
    visit_date: Optional[str] = None,
    start_from_scratch: Optional[bool] = False,
):
    """Import a svn repository

    Args:
        - url: (mandatory) svn's repository url to ingest data from
        - origin_url: Optional original url override to use as origin reference
            in the archive. If not provided, "url" is used as origin.
        - destination_path: (optional) root directory to
          locally retrieve svn's data
        - swh_revision: (optional) extra revision hex to
          start from. See swh.loader.svn.SvnLoader.process
          docstring
        - visit_date: Optional date to override the visit date
        - start_from_scratch: Flag to allow starting back the svn repository from the
          start

    """
    loader = SvnLoader.from_configfile(
        url=url,
        origin_url=origin_url,
        destination_path=destination_path,
        swh_revision=swh_revision,
        visit_date=convert_to_datetime(visit_date),
        start_from_scratch=start_from_scratch,
    )
    return loader.load()


@shared_task(name=__name__ + ".MountAndLoadSvnRepository")
def load_svn_from_archive(
    *,
    url: Optional[str] = None,
    archive_path: Optional[str] = None,
    visit_date: Optional[str] = None,
    start_from_scratch: Optional[bool] = False,
):
    """1. Mount an svn dump from archive as a local svn repository
       2. Load it through the svn loader
       3. Clean up mounted svn repository archive

    Args:
        - url: origin url
        - archive_path: Path on disk to the archive holdin the svn repository to ingest
        - visit_date: Optional date to override the visit date
        - start_from_scratch: Flag to allow starting back the svn repository from the
          start

    """
    loader = SvnLoaderFromDumpArchive.from_configfile(
        url=url,
        archive_path=archive_path,
        visit_date=convert_to_datetime(visit_date),
        start_from_scratch=start_from_scratch,
    )
    return loader.load()


@shared_task(name=__name__ + ".DumpMountAndLoadSvnRepository")
def load_svn_from_remote_dump(
    *,
    url: Optional[str] = None,
    origin_url: Optional[str] = None,
    visit_date: Optional[str] = None,
    start_from_scratch: Optional[bool] = False,
):
    """1. Mount a remote svn dump as a local svn repository.
       2. Load it through the svn loader.
       3. Clean up mounted svn repository archive.

    Args:
        - url: (mandatory) svn's repository url to ingest data from
        - origin_url: Optional original url override to use as origin reference
            in the archive. If not provided, "url" is used as origin.
        - visit_date: Optional date to override the visit date
        - start_from_scratch: Flag to allow starting back the svn repository from the
          start

    """
    loader = SvnLoaderFromRemoteDump.from_configfile(
        url=url,
        origin_url=origin_url,
        visit_date=convert_to_datetime(visit_date),
        start_from_scratch=start_from_scratch,
    )
    return loader.load()
