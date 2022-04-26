# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import shared_task

from .loader import SvnLoader, SvnLoaderFromDumpArchive, SvnLoaderFromRemoteDump


@shared_task(name=__name__ + ".LoadSvnRepository")
def load_svn(*args, **kwargs):
    """Import a svn repository"""
    loader = SvnLoader.from_configfile(*args, **kwargs)
    return loader.load()


@shared_task(name=__name__ + ".MountAndLoadSvnRepository")
def load_svn_from_archive(*args, **kwargs):
    """
    1. Mount an svn dump from archive as a local svn repository
    2. Load it through the svn loader
    3. Clean up mounted svn repository archive
    """
    loader = SvnLoaderFromDumpArchive.from_configfile(*args, **kwargs)
    return loader.load()


@shared_task(name=__name__ + ".DumpMountAndLoadSvnRepository")
def load_svn_from_remote_dump(*args, **kwargs):
    """
    1. Mount a remote svn dump as a local svn repository.
    2. Load it through the svn loader.
    3. Clean up mounted svn repository archive.
    """
    loader = SvnLoaderFromRemoteDump.from_configfile(*args, **kwargs)
    return loader.load()
