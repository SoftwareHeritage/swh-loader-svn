# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import email.utils


def uid_to_person(uid, encode=True):
    """Convert an uid to a person suitable for insertion.

    Args:
        uid: an uid of the form "Name <email@ddress>"
        encode: whether to convert the output to bytes or not
    Returns: a dictionary with keys:
        name: the name associated to the uid
        email: the mail associated to the uid
    """

    ret = {
        'name': '',
        'email': '',
    }

    name, mail = email.utils.parseaddr(uid)

    if name and email:
        ret['name'] = name
        ret['email'] = mail
    else:
        ret['name'] = uid

    if encode:
        for key in ('name', 'email'):
            ret[key] = ret[key].encode('utf-8')

    return ret


def build_swh_revision(repo_uuid, commit, rev, dir_id, parents):
    """Given a svn revision, build a swh revision.

    """
    author = uid_to_person(commit['author_name'])

    msg = commit['message'].encode('utf-8')

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
        'author': author,
        'committer': author,
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
