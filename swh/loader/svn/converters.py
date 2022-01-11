# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Dict, Optional, Sequence, Tuple

import iso8601

from swh.model.model import Person, Revision, RevisionType, TimestampWithTimezone


def svn_date_to_swh_date(strdate: Optional[bytes]) -> TimestampWithTimezone:
    """Convert a string date to an swh one.

    Args:
        strdate: A string representing a date with format like
        ``b'YYYY-mm-DDTHH:MM:SS.800722Z'``

    Returns:
        An swh date format

    """
    if not strdate:  # either None or empty string
        dt = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    else:
        dt = iso8601.parse_date(strdate.decode("ascii"))
        assert dt.tzinfo is not None, strdate
    return TimestampWithTimezone.from_datetime(dt)


def svn_author_to_swh_person(author: Optional[bytes]) -> Person:
    """Convert an svn author to an swh person.
    Default policy: No information is added.

    Args:
        author: the svn author (in bytes)

    Returns:
        a Person

    """
    return Person.from_fullname(author or b"")


def build_swh_revision(
    rev: int, commit: Dict, repo_uuid: bytes, dir_id: bytes, parents: Sequence[bytes]
) -> Revision:
    """Given a svn revision, build a swh revision.

    This adds an 'extra-headers' entry with the
    repository's uuid and the svn revision.

    Args:
        rev: the svn revision number
        commit: the commit data: revision id, date, author, and message
        repo_uuid: The repository's uuid
        dir_id: the tree's hash identifier
        parents: the revision's parents identifier

    Returns:
        The swh revision dictionary.

    """
    author = commit["author_name"]
    msg = commit["message"]
    date = commit["author_date"]

    extra_headers: Tuple[Tuple[bytes, bytes], ...] = (
        (b"svn_repo_uuid", repo_uuid),
        (b"svn_revision", str(rev).encode()),
    )

    return Revision(
        type=RevisionType.SUBVERSION,
        date=date,
        committer_date=date,
        directory=dir_id,
        message=msg,
        author=author,
        committer=author,
        synthetic=True,
        extra_headers=extra_headers,
        parents=tuple(parents),
    )
