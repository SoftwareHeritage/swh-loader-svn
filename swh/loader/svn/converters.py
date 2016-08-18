# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from email import utils

from swh.core import hashutil

from .utils import strdate_to_timestamp


def svn_date_to_gitsvn_date(strdate):
    """Convert a string date to an swh one.

    Args:
        strdate: A string formatted for .utils.strdate_to_timestamp
        to do its jobs

    Returns:
        An swh date format with an integer timestamp.

    """
    return {
        'timestamp': int(strdate_to_timestamp(strdate)),
        'offset': 0
    }


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

    author = author.encode('utf-8')
    if b'<' in author and b'>' in author:
        name, email = utils.parseaddr(author.decode('utf-8'))
        return {
            'fullname': author,
            'name': name.encode('utf-8'),
            'email': email.encode('utf-8')
        }

    return {'fullname': author, 'email': None, 'name': author}


def svn_author_to_gitsvn_person(author, repo_uuid):
    """Convert an svn author to a person suitable for insertion.

    Default policy: If no email is found, the email is created using
    the author and the repo_uuid.

    Args:
        author (string): the svn author (in bytes)
        repo_uuid (bytes): the repository's uuid

    Returns: a dictionary with keys:
        fullname: the author's associated fullname
        name: the author's associated name
        email: None (no email in svn)

    """
    if not author:
        author = '(no author)'

    author = author.encode('utf-8')
    if b'<' in author and b'>' in author:
        name, email = utils.parseaddr(author.decode('utf-8'))
        return {
            'fullname': author,
            'name': name.encode('utf-8'),
            'email': email.encode('utf-8')
        }

    # we'll construct the author's fullname the same way git svn does
    # 'user <user@repo-uuid>'

    email = b'@'.join([author, repo_uuid])
    return {
        'fullname': b''.join([author, b' ', b'<', email, b'>']),
        'name': author,
        'email': email,
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


def build_gitsvn_swh_revision(rev, commit, dir_id, parents):
    """Given a svn revision, build a swh revision.

    Args:
        - rev: the svn revision number
        - commit: the commit metadata
        - dir_id: the tree's hash identifier
        - parents: the revision's parents identifier

    Returns:
        The swh revision dictionary.
    """
    author = commit['author_name']
    msg = commit['message']
    date = commit['author_date']

    return {
        'date': date,
        'committer_date': date,
        'type': 'svn',
        'directory': dir_id,
        'message': msg,
        'author': author,
        'committer': author,
        'synthetic': True,
        'metadata': None,
        'parents': parents,
    }


def build_swh_occurrence(revision_id, origin_id, date):
    """Build a swh occurrence from the revision id, origin id, and date.

    """
    return {'branch': 'master',
            'target': revision_id,
            'target_type': 'revision',
            'origin': origin_id,
            'date': date}


def loader_to_scheduler_revision(swh_revision):
    """To avoid serialization or scheduler storage problem, transform
    adequately the revision.

    FIXME: Should be more generically dealt with in swh-scheduler's
    side.  The advantage to having it here is that we known what we
    store.

    """
    if not swh_revision:
        return None

    metadata = swh_revision['metadata']
    for entry in (e for e in metadata['extra_headers']
                  if isinstance(e[1], bytes)):
        entry[1] = entry[1].decode('utf-8')

    return {
        'id': hashutil.hash_to_hex(swh_revision['id']),
        'parents': [hashutil.hash_to_hex(parent) for parent
                    in swh_revision['parents']],
        'metadata': metadata
    }


def scheduler_to_loader_revision(swh_revision):
    """If the known state (a revision) is already passed, it will be
    serializable ready but not loader ready.

    FIXME: Should be more generically dealt with in swh-scheduler's
    side.  The advantage to having it here is that we known what we
    store.

    """
    if not swh_revision:
        return None
    return {
        'id': hashutil.hex_to_hash(swh_revision['id']),
        'parents': [hashutil.hex_to_hash(parent) for parent
                    in swh_revision['parents']],
        'metadata': swh_revision['metadata']
    }
