# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def svn_author_to_person(author):
    """Convert an svn author to a person suitable for insertion.

    Args:
        author: the svn author (in bytes)

    Returns: a dictionary with keys:
        fullname: the name associate to the author
        name: the name associated to the author
        email: None (no email in svn)

    """
    return {
        'fullname': author,
        'name': author,
        'email': None,
    }


def build_swh_revision(repo_uuid, commit, rev, dir_id, parents,
                       with_extra_headers=True):
    """Given a svn revision, build a swh revision.

    """
    author = svn_author_to_person(commit['author_name'])

    msg = commit['message']

    date = {
        'timestamp': commit['author_date'],
        'offset': 0,
    }

    if with_extra_headers:
        metadata = {
            'extra_headers': [
                ['svn_repo_uuid', repo_uuid],
                ['svn_revision', rev]
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
