# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import psycopg2
import requests
import traceback
import uuid

from retrying import retry

from swh.core import config

from swh.loader.dir import converters
from swh.model.git import GitType
from swh.storage import get_storage

from swh.loader.svn.queue import QueuePerSize, QueuePerNbElements


def retry_loading(error):
    """Retry policy when the database raises an integrity error"""
    exception_classes = [
        # raised when two parallel insertions insert the same data.
        psycopg2.IntegrityError,
        # raised when uWSGI restarts and hungs up on the worker.
        requests.exceptions.ConnectionError,
    ]

    if not any(isinstance(error, exc) for exc in exception_classes):
        return False

    logger = logging.getLogger('swh.loader')

    error_name = error.__module__ + '.' + error.__class__.__name__
    logger.warning('Retry loading a batch', exc_info=False, extra={
        'swh_type': 'storage_retry',
        'swh_exception_type': error_name,
        'swh_exception': traceback.format_exception(
            error.__class__,
            error,
            error.__traceback__,
        ),
    })

    return True


class SWHLoader(config.SWHConfig):
    """A svn loader.

    This will load the svn repository.

    """
    def __init__(self, config, revision_type, origin_id, logging_class):
        self.config = config

        self.origin_id = origin_id
        self.storage = get_storage(config['storage_class'],
                                   config['storage_args'])
        self.revision_type = revision_type

        self.log = logging.getLogger(logging_class)

        self.contents = QueuePerSize(key='sha1',
                                     max_nb_elements=self.config[
                                         'content_packet_size'],
                                     max_size=self.config[
                                         'content_packet_block_size_bytes'])

        self.directories = QueuePerNbElements(key='id',
                                              max_nb_elements=self.config[
                                                 'directory_packet_size'])

        self.revisions = QueuePerNbElements(key='id',
                                            max_nb_elements=self.config[
                                               'revision_packet_size'])

        self.releases = QueuePerNbElements(key='id',
                                           max_nb_elements=self.config[
                                               'release_packet_size'])

        self.occurrences = QueuePerNbElements(key='id',
                                              max_nb_elements=self.config[
                                                'occurrence_packet_size'])

        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_contents(self, content_list):
        """Actually send properly formatted contents to the database"""
        num_contents = len(content_list)
        if num_contents > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d contents" % num_contents,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'content',
                               'swh_num': num_contents,
                               'swh_id': log_id,
                           })
            self.storage.content_add(content_list)
            self.log.debug("Done sending %d contents" % num_contents,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'content',
                               'swh_num': num_contents,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_directories(self, directory_list):
        """Actually send properly formatted directories to the database"""
        num_directories = len(directory_list)
        if num_directories > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d directories" % num_directories,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'directory',
                               'swh_num': num_directories,
                               'swh_id': log_id,
                           })
            self.storage.directory_add(directory_list)
            self.log.debug("Done sending %d directories" % num_directories,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'directory',
                               'swh_num': num_directories,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_revisions(self, revision_list):
        """Actually send properly formatted revisions to the database"""
        num_revisions = len(revision_list)
        if num_revisions > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d revisions" % num_revisions,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'revision',
                               'swh_num': num_revisions,
                               'swh_id': log_id,
                           })
            self.storage.revision_add(revision_list)
            self.log.debug("Done sending %d revisions" % num_revisions,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'revision',
                               'swh_num': num_revisions,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_releases(self, release_list):
        """Actually send properly formatted releases to the database"""
        num_releases = len(release_list)
        if num_releases > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d releases" % num_releases,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'release',
                               'swh_num': num_releases,
                               'swh_id': log_id,
                           })
            self.storage.release_add(release_list)
            self.log.debug("Done sending %d releases" % num_releases,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'release',
                               'swh_num': num_releases,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_occurrences(self, occurrence_list):
        """Actually send properly formatted occurrences to the database"""
        num_occurrences = len(occurrence_list)
        if num_occurrences > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d occurrences" % num_occurrences,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'occurrence',
                               'swh_num': num_occurrences,
                               'swh_id': log_id,
                           })
            self.storage.occurrence_add(occurrence_list)
            self.log.debug("Done sending %d occurrences" % num_occurrences,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'occurrence',
                               'swh_num': num_occurrences,
                               'swh_id': log_id,
                           })

    def shallow_blob(self, obj):
        return {
            'sha1': obj['sha1'],
            'sha256': obj['sha256'],
            'sha1_git': obj['sha1_git'],
            'length': obj['length']
        }

    def filter_missing_blobs(self, blobs):
        """Filter missing blob from swh.

        """
        max_content_size = self.config['content_packet_size_bytes']
        blobs_per_sha1 = {}
        for blob in blobs:
            blobs_per_sha1[blob['sha1']] = blob

        for sha1 in self.storage.content_missing((self.shallow_blob(b)
                                                 for b in blobs),
                                                 key_hash='sha1'):
            yield converters.blob_to_content(blobs_per_sha1[sha1],
                                             max_content_size=max_content_size,
                                             origin_id=self.origin_id)

    def bulk_send_blobs(self, blobs):
        """Format blobs as swh contents and send them to the database"""
        threshold_reached = self.contents.add(
            self.filter_missing_blobs(blobs))
        if threshold_reached:
            self.send_contents(self.contents.pop())

    def shallow_tree(self, tree):
        return tree['sha1_git']

    def filter_missing_trees(self, trees, objects):
        """Filter missing tree from swh.

        """
        trees_per_sha1 = {}
        for tree in trees:
            trees_per_sha1[tree['sha1_git']] = tree

        for sha in self.storage.directory_missing((self.shallow_tree(b)
                                                   for b in trees)):
            yield converters.tree_to_directory(trees_per_sha1[sha], objects)

    def bulk_send_trees(self, objects, trees):
        """Format trees as swh directories and send them to the database"""
        threshold_reached = self.directories.add(
            self.filter_missing_trees(trees, objects))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())

    def shallow_commit(self, commit):
        return commit['id']

    def filter_missing_commits(self, commits):
        """Filter missing commit from swh.

        """
        commits_per_sha1 = {}
        for commit in commits:
            commits_per_sha1[commit['id']] = commit

        for sha in self.storage.revision_missing((self.shallow_commit(b)
                                                  for b in commits),
                                                 type=self.revision_type):
            yield commits_per_sha1[sha]

    def bulk_send_commits(self, commits):
        """Format commits as swh revisions and send them to the database.

        """
        threshold_reached = self.revisions.add(
            self.filter_missing_commits(commits))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())

    def bulk_send_annotated_tags(self, tags):
        """Format annotated tags (pygit2.Tag objects) as swh releases and send
        them to the database.

        """
        threshold_reached = self.releases.add(tags)
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())
            self.send_releases(self.releases.pop())

    def bulk_send_refs(self, refs):
        """Format git references as swh occurrences and send them to the
        database.

        """
        threshold_reached = self.occurrences.add(
            map(converters.ref_to_occurrence, refs))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())
            self.send_releases(self.releases.pop())
            self.send_occurrences(self.occurrences.pop())

    def maybe_load_contents(self, contents):
        if self.config['send_contents']:
            self.bulk_send_blobs(contents)
        else:
            self.log.info('Not sending contents')

    def maybe_load_directories(self, trees, objects_per_path):
        if self.config['send_directories']:
            self.bulk_send_trees(objects_per_path, trees)
        else:
            self.log.info('Not sending directories')

    def maybe_load_revisions(self, revisions):
        if self.config['send_revisions']:
            self.bulk_send_commits(revisions)
        else:
            self.log.info('Not sending revisions')

    def maybe_load_releases(self, releases):
        if self.config['send_releases']:
            self.bulk_send_annotated_tags(releases)
        else:
            self.log.info('Not sending releases')

    def maybe_load_occurrences(self, occurrences):
        if self.config['send_occurrences']:
            self.bulk_send_refs(occurrences)
        else:
            self.log.info('Not sending occurrences')

    def load(self, objects_per_type, objects_per_path):
        self.maybe_load_contents(objects_per_type[GitType.BLOB])
        self.maybe_load_directories(objects_per_type[GitType.TREE],
                                    objects_per_path)
        self.maybe_load_revisions(objects_per_type[GitType.COMM])
        self.maybe_load_releases(objects_per_type[GitType.RELE])
        self.maybe_load_occurrences(objects_per_type[GitType.REFS])

    def flush(self):
        if self.config['send_contents']:
            self.send_contents(self.contents.pop())
        if self.config['send_directories']:
            self.send_directories(self.directories.pop())
        if self.config['send_revisions']:
            self.send_revisions(self.revisions.pop())
        if self.config['send_occurrences']:
            self.send_occurrences(self.occurrences.pop())
        if self.config['send_releases']:
            self.send_releases(self.releases.pop())
