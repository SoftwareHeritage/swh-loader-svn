# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting tree at a specific revision.

"""

from pathlib import Path
from typing import Iterator, Optional

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.svn.svn_repo import SvnRepo, get_svn_repo
from swh.model.model import Snapshot, SnapshotBranch, TargetType


class SvnExportLoader(BaseDirectoryLoader):
    """Svn export (of a tree) loader at a specific svn revision or tag (release) into
    the swh archive.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: alias
           target: rev_<svn-revision>
         rev_<svn-revision>:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "svn-export"

    def __init__(self, *args, **kwargs):
        self.svn_revision = kwargs.pop("ref")
        super().__init__(*args, **kwargs)
        self.svnrepo: Optional[SvnRepo] = None

    def prepare(self) -> None:
        self.svnrepo = get_svn_repo(self.origin.url)
        super().prepare()

    def cleanup(self) -> None:
        """Clean up any intermediary fs."""
        if self.svnrepo:
            self.svnrepo.clean_fs()

    def fetch_artifact(self) -> Iterator[Path]:
        """Prepare the svn local repository checkout at a given commit/tag."""
        assert self.svnrepo is not None
        _, local_url = self.svnrepo.export_temporary(self.svn_revision)
        yield Path(local_url.decode())

    def build_snapshot(self) -> Snapshot:
        """Build snapshot without losing the svn revision context."""
        assert self.directory is not None
        branch_name = f"rev_{self.svn_revision}".encode()
        return Snapshot(
            branches={
                b"HEAD": SnapshotBranch(
                    target_type=TargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target_type=TargetType.DIRECTORY,
                    target=self.directory.hash,
                ),
            }
        )
