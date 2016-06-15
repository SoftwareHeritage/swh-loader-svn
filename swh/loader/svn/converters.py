# Copyright (C) 2015-2016  The Software Heritage developers
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


def svn_author_to_person(author, repo_uuid):
    """Convert an svn author to a person suitable for insertion.

    Args:
        author (bytes): the svn author (in bytes)
        repo_uuid (string): the repository's uuid

    Returns: a dictionary with keys:
        fullname: the author's associated fullname
        name: the author's associated name
        email: None (no email in svn)

    """
    if not author:
        return {
            'fullname': b'',
            'name': None,
            'email': None,
        }

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


def build_swh_revision(repo_uuid, commit, rev, dir_id, parents,
                       with_revision_headers=True):
    """Given a svn revision, build a swh revision.

    """
    author = commit['author_name']
    msg = commit['message']
    date = commit['author_date']

    if with_revision_headers:
        metadata = {
            'extra_headers': [
                ['svn_repo_uuid', repo_uuid],
                ['svn_revision', str(rev).encode('utf-8')]
            ]
        }
    else:
        metadata = None

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


def build_swh_occurrence(revision_id, origin_id, date):
    """Build a swh occurrence from the revision id, origin id, and date.

    """
    return {'branch': 'master',
            'target': revision_id,
            'target_type': 'revision',
            'origin': origin_id,
            'date': date}
