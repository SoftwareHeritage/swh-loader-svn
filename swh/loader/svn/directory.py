# Copyright (C) 2023-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting tree at a specific revision."""

from datetime import datetime
import os
from pathlib import Path
import tempfile
from typing import Iterator, List, Optional

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.svn.svn_repo import SvnRepo, get_svn_repo
from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType


class SvnExportLoader(BaseDirectoryLoader):
    """Load a svn tree at a specific svn revision into the swh archive.

    It is also possible to load a subset of the source tree by explicitly
    specifying the sub-paths to export in the ``svn_paths`` optional parameter.

    If the origin URL should be different from the subversion URL, the latter
    can be provided using the optional ``svn_url`` parameter.

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

    def __init__(
        self,
        *args,
        svn_paths: Optional[List[str]] = None,
        svn_url: Optional[str] = None,
        **kwargs,
    ):
        self.svn_revision = int(kwargs.pop("ref"))
        self.svn_paths = svn_paths
        super().__init__(*args, **kwargs)
        self.svn_url = svn_url
        if self.svn_url is None:
            self.svn_url = self.origin.url
        self.svnrepo: Optional[SvnRepo] = None

    def prepare(self) -> None:
        self.svnrepo = get_svn_repo(self.svn_url, revision=self.svn_revision)
        super().prepare()

    def cleanup(self) -> None:
        """Clean up any intermediary fs."""
        if self.svnrepo:
            self.svnrepo.clean_fs()

    def fetch_artifact(self) -> Iterator[Path]:
        """Prepare the svn local repository checkout at a given commit/tag."""
        assert self.svnrepo is not None
        if self.svn_paths is None:
            _, local_url = self.svnrepo.export_temporary(self.svn_revision)
            yield Path(local_url.decode())
        else:
            assert self.svn_url is not None
            self.log.debug(
                "Exporting from the svn source tree rooted at %s@%s the sub-paths: %s",
                self.svn_url,
                self.svn_revision,
                ", ".join(self.svn_paths),
            )
            with tempfile.TemporaryDirectory(
                suffix="-" + datetime.now().isoformat()
            ) as tmp_dir:
                for svn_path in self.svn_paths:
                    svn_url = os.path.join(self.svn_url, svn_path.strip("/"))
                    export_path = os.path.join(tmp_dir, svn_path.strip("/"))
                    os.makedirs("/".join(export_path.split("/")[:-1]), exist_ok=True)
                    self.svnrepo.export(
                        svn_url,
                        export_path,
                        rev=int(self.svn_revision),
                        remove_dest_path=False,
                        overwrite=True,
                        ignore_externals=True,
                        ignore_keywords=True,
                    )
                yield Path(tmp_dir)

    def build_snapshot(self) -> Snapshot:
        """Build snapshot without losing the svn revision context."""
        assert self.directory is not None
        branch_name = f"rev_{self.svn_revision}".encode()
        return Snapshot(
            branches={
                b"HEAD": SnapshotBranch(
                    target_type=SnapshotTargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target_type=SnapshotTargetType.DIRECTORY,
                    target=self.directory.hash,
                ),
            }
        )
