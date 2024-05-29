# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import shared_task

from swh.loader.core.utils import parse_visit_date

from .directory import SvnExportLoader
from .loader import SvnLoaderFromDump, SvnLoaderFromRemoteDump


def _process_kwargs(kwargs):
    if "visit_date" in kwargs:
        kwargs["visit_date"] = parse_visit_date(kwargs["visit_date"])
    return kwargs


@shared_task(name=__name__ + ".DumpMountAndLoadSvnRepository")
def load_svn(**kwargs):
    """
    1. Mount a remote svn dump as a local svn repository.
    2. Load it through the svn loader.
    3. Clean up mounted svn repository archive.
    """
    loader = SvnLoaderFromRemoteDump.from_configfile(**_process_kwargs(kwargs))
    return loader.load()


@shared_task(name=__name__ + ".MountAndLoadSvnRepository")
def load_svn_from_dump(**kwargs):
    """
    1. Mount a (possibly gzip compressed) svn dump as a local svn repository.
    2. Load it through the svn loader.
    """
    loader = SvnLoaderFromDump.from_configfile(**_process_kwargs(kwargs))
    return loader.load()


@shared_task(name=f"{__name__}.LoadSvnExport")
def load_svn_export(**kwargs):
    """Load svn tree into the swh archive."""
    loader = SvnExportLoader.from_configfile(**_process_kwargs(kwargs))
    return loader.load()
