# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.svn import converters
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Person, Revision, Timestamp, TimestampWithTimezone


def test_svn_author_to_swh_person():
    """The author should have name, email and fullname filled."""
    actual_person = converters.svn_author_to_swh_person(b"tony <ynot@dagobah>")

    assert actual_person == Person.from_dict(
        {
            "fullname": b"tony <ynot@dagobah>",
            "name": b"tony",
            "email": b"ynot@dagobah",
        }
    )


def test_svn_author_to_swh_person_no_email():
    """The author and fullname should be the same as the input (author)."""
    actual_person = converters.svn_author_to_swh_person(b"tony")
    assert actual_person == Person.from_dict(
        {
            "fullname": b"tony",
            "name": b"tony",
            "email": None,
        }
    )


def test_svn_author_to_swh_person_empty_person():
    """Empty person has only its fullname filled with the empty
    byte-string.

    """
    actual_person = converters.svn_author_to_swh_person(b"")
    assert actual_person == Person.from_dict(
        {
            "fullname": b"",
            "name": None,
            "email": None,
        }
    )


def test_build_swh_revision_default():
    """This should build the swh revision with the swh revision's extra
    headers about the repository.

    """
    dir_id = hash_to_bytes("d6e08e19159f77983242877c373c75222d5ae9dd")
    date = TimestampWithTimezone(
        timestamp=Timestamp(seconds=1088108379, microseconds=0), offset_bytes=b"+0000"
    )
    actual_rev = converters.build_swh_revision(
        repo_uuid=b"uuid",
        dir_id=dir_id,
        commit={
            "author_name": Person(
                name=b"theo", email=b"theo@uuid", fullname=b"theo <theo@uuid>"
            ),
            "message": b"commit message",
            "author_date": date,
        },
        rev=10,
        parents=(),
    )

    expected_rev = Revision.from_dict(
        {
            "date": date.to_dict(),
            "committer_date": date.to_dict(),
            "type": "svn",
            "directory": dir_id,
            "message": b"commit message",
            "author": {
                "name": b"theo",
                "email": b"theo@uuid",
                "fullname": b"theo <theo@uuid>",
            },
            "committer": {
                "name": b"theo",
                "email": b"theo@uuid",
                "fullname": b"theo <theo@uuid>",
            },
            "synthetic": True,
            "extra_headers": (
                (b"svn_repo_uuid", b"uuid"),
                (b"svn_revision", b"10"),
            ),
            "parents": (),
        }
    )

    assert actual_rev == expected_rev


def test_svn_date_to_swh_date():
    """The timestamp should not be tampered with and include the
    decimals.

    """
    assert converters.svn_date_to_swh_date(
        b"2011-05-31T06:04:39.500900Z"
    ) == TimestampWithTimezone(
        timestamp=Timestamp(seconds=1306821879, microseconds=500900),
        offset_bytes=b"+0000",
    )

    assert converters.svn_date_to_swh_date(
        b"2011-05-31T06:04:39.800722Z"
    ) == TimestampWithTimezone(
        timestamp=Timestamp(seconds=1306821879, microseconds=800722),
        offset_bytes=b"+0000",
    )


def test_svn_date_to_swh_date_epoch():
    """Empty date should be EPOCH (timestamp and offset at 0)."""
    # It should return 0, epoch
    default_tstz = TimestampWithTimezone(
        timestamp=Timestamp(seconds=0, microseconds=0), offset_bytes=b"+0000"
    )

    assert converters.svn_date_to_swh_date("") == default_tstz
    assert converters.svn_date_to_swh_date(None) == default_tstz
