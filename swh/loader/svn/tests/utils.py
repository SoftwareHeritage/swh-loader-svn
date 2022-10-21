# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum
from io import BytesIO
import os
from typing import Dict, List

from subvertpy import SubversionException, delta, repos
from subvertpy.ra import Auth, RemoteAccess, get_username_provider
from typing_extensions import TypedDict


class CommitChangeType(Enum):
    AddOrUpdate = 1
    Delete = 2


class CommitChange(TypedDict, total=False):
    change_type: CommitChangeType
    path: str
    properties: Dict[str, str]
    data: bytes
    copyfrom_path: str
    copyfrom_rev: int


def add_commit(repo_url: str, message: str, changes: List[CommitChange]) -> None:
    conn = RemoteAccess(repo_url, auth=Auth([get_username_provider()]))
    editor = conn.get_commit_editor({"svn:log": message})
    root = editor.open_root()
    for change in changes:
        if change["change_type"] == CommitChangeType.Delete:
            root.delete_entry(change["path"].rstrip("/"))
        else:
            dir_change = change["path"].endswith("/")
            split_path = change["path"].rstrip("/").split("/")
            copyfrom_path = change.get("copyfrom_path")
            copyfrom_rev = change.get("copyfrom_rev", -1)
            for i in range(len(split_path)):
                path = "/".join(split_path[0 : i + 1])
                if i < len(split_path) - 1:
                    try:
                        root.add_directory(path, copyfrom_path, copyfrom_rev).close()
                    except SubversionException:
                        pass
                else:
                    if dir_change:
                        try:
                            dir = root.add_directory(path, copyfrom_path, copyfrom_rev)
                        except SubversionException:
                            dir = root.open_directory(path)
                        if "properties" in change:
                            for prop, value in change["properties"].items():
                                dir.change_prop(prop, value)
                        dir.close()
                    else:
                        try:
                            file = root.add_file(path, copyfrom_path, copyfrom_rev)
                        except SubversionException:
                            file = root.open_file(path)
                        if "properties" in change:
                            for prop, value in change["properties"].items():
                                file.change_prop(prop, value)
                        if "data" in change:
                            txdelta = file.apply_textdelta()
                            delta.send_stream(BytesIO(change["data"]), txdelta)
                        file.close()
    root.close()
    editor.close()


def create_repo(tmp_path, repo_name="tmprepo"):
    repo_path = os.path.join(tmp_path, repo_name)
    repos.create(repo_path)
    return f"file://{repo_path}"
