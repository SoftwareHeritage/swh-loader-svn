# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from email import utils

from .utils import strdate_to_timestamp


def svn_date_to_swh_date(strdate):
    """Convert a string date to an swh one.

    Args:
        strdate: A string formatted for .utils.strdate_to_timestamp
        to do its jobs

    Returns:
        An swh date format

    """
    return {
        'timestamp': strdate_to_timestamp(strdate),
        'offset': 0
    }


def svn_author_to_swh_person(author):
    """Convert an svn author to an swh person.
    Default policy: No information is added.

    Args:
        author (string): the svn author (in bytes)

    Returns: a dictionary with keys:
        fullname: the author's associated fullname
        name: the author's associated name
        email: None (no email in svn)

    """
    if not author:
        return {'fullname': b'', 'name': None, 'email': None}

    if isinstance(author, str):
        author = author.encode('utf-8')

    if b'<' in author and b'>' in author:
        name, email = utils.parseaddr(author.decode('utf-8'))
        return {
            'fullname': author,
            'name': name.encode('utf-8'),
            'email': email.encode('utf-8')
        }

    return {'fullname': author, 'email': None, 'name': author}


    }


def build_swh_revision(rev, commit, repo_uuid, dir_id, parents):
    """Given a svn revision, build a swh revision.

    This adds an ['metadata']['extra-headers'] entry with the
    repository's uuid and the svn revision.

    Args:
        - rev: the svn revision number
        - commit: the commit metadata
        - repo_uuid: The repository's uuid
        - dir_id: the tree's hash identifier
        - parents: the revision's parents identifier

    Returns:
        The swh revision dictionary.

    """
    author = commit['author_name']
    msg = commit['message']
    date = commit['author_date']

    metadata = {
        'extra_headers': [
            ['svn_repo_uuid', repo_uuid],
            ['svn_revision', str(rev).encode('utf-8')]
        ]
    }

    return {
        'date': date,
        'committer_date': date,
        'type': 'svn',
        'directory': dir_id,
        'message': msg,
        'author': author,
        'committer': author,
        'synthetic': True,
        'metadata': metadata,
        'parents': parents,
    }
