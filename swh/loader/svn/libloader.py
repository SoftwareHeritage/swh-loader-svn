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


def send_in_packets(source_list, formatter, sender, packet_size,
                    packet_size_bytes=None, *args, **kwargs):
    """Send objects from `source_list`, passed through `formatter` (with
    extra args *args, **kwargs), using the `sender`, in packets of
    `packet_size` objects (and of max `packet_size_bytes`).

    """
    formatted_objects = []
    count = 0
    if not packet_size_bytes:
        packet_size_bytes = 0
    for obj in source_list:
        formatted_object = formatter(obj, *args, **kwargs)
        if formatted_object:
            formatted_objects.append(formatted_object)
        else:
            continue
        if packet_size_bytes:
            count += formatted_object['length']
        if len(formatted_objects) >= packet_size or count > packet_size_bytes:
            sender(formatted_objects)
            formatted_objects = []
            count = 0

    if formatted_objects:
        sender(formatted_objects)


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
    def __init__(self, config, logging_class):
        self.config = config

        if self.config['storage_class'] == 'remote_storage':
            from swh.storage.api.client import RemoteStorage as Storage
        else:
            from swh.storage import Storage

        self.storage = Storage(*self.config['storage_args'])

        self.log = logging.getLogger(logging_class)

        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_contents(self, content_list):
        """Actually send properly formatted contents to the database"""
        num_contents = len(content_list)
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

    def bulk_send_blobs(self, objects, blobs, origin_id):
        """Format blobs as swh contents and send them to the database"""
        packet_size = self.config['content_packet_size']
        packet_size_bytes = self.config['content_packet_size_bytes']
        max_content_size = self.config['content_size_limit']

        send_in_packets(blobs, converters.blob_to_content,
                        self.send_contents, packet_size,
                        packet_size_bytes=packet_size_bytes,
                        log=self.log,
                        max_content_size=max_content_size,
                        origin_id=origin_id)

    def bulk_send_trees(self, objects, trees):
        """Format trees as swh directories and send them to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(trees, converters.tree_to_directory,
                        self.send_directories, packet_size,
                        objects=objects,
                        log=self.log)

    def bulk_send_commits(self, commits):
        """Format commits as swh revisions and send them to the database.

        """
        packet_size = self.config['revision_packet_size']

        send_in_packets(commits, (lambda x, objects={}, log=None: x),
                        self.send_revisions, packet_size,
                        log=self.log)

    def bulk_send_annotated_tags(self, tags):
        """Format annotated tags (pygit2.Tag objects) as swh releases and send
        them to the database.

        """
        packet_size = self.config['release_packet_size']

        send_in_packets(tags, (lambda x, objects={}, log=None: x),
                        self.send_releases, packet_size,
                        log=self.log)

    def bulk_send_refs(self, refs):
        """Format git references as swh occurrences and send them to the
        database.

        """
        packet_size = self.config['occurrence_packet_size']
        send_in_packets(refs, converters.ref_to_occurrence,
                        self.send_occurrences, packet_size)

    def maybe_load_contents(self, contents, objects_per_path, origin_id):
        if self.config['send_contents']:
            self.bulk_send_blobs(objects_per_path, contents, origin_id)
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

    def load(self, objects_per_type, objects_per_path, origin_id):
        self.maybe_load_contents(objects_per_type[GitType.BLOB],
                                 objects_per_path,
                                 origin_id)
        self.maybe_load_directories(objects_per_type[GitType.TREE],
                                    objects_per_path)
        self.maybe_load_revisions(objects_per_type[GitType.COMM])
        self.maybe_load_releases(objects_per_type[GitType.RELE])
        self.maybe_load_occurrences(objects_per_type[GitType.REFS])
