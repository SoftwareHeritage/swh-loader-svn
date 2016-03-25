# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def build_swh_revision(repo_uuid, commit, rev, dir_id, parents):
    """Given a svn revision, build a swh revision.

    """
    author = commit['author_name']
    if author:
        author_committer = {
            # HACK: shouldn't we use the same for email?
            'name': author.encode('utf-8'),
            'email': b'',
        }
    else:
        author_committer = {
            'name': b'',  # HACK: some repository have commits without author
            'email': b'',
        }

    msg = commit['message']
    if msg:
        msg = msg.encode('utf-8')
    else:
        msg = b''

    date = {
        'timestamp': commit['author_date'],
        'offset': 0,
    }

    return {
        'date': date,
        'committer_date': date,
        'type': 'svn',
        'directory': dir_id,
        'message': msg,
        'author': author_committer,
        'committer': author_committer,
        'synthetic': True,
        'metadata': {
            'extra_headers': {
                'svn_repo_uuid': repo_uuid,
                'svn_revision': rev,
            }
        },
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
