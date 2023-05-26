# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from typing_extensions import TypedDict

class SvnPathInfo(TypedDict):
    type: str
    props: Dict[str, str]

def crawl_repository(
    repo_url: str, revnum: int = -1, username: str = "", password: str = ""
) -> Dict[str, SvnPathInfo]: ...
