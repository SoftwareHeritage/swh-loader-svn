# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn.queue import QueuePerNbElements
from swh.loader.svn.queue import QueuePerNbUniqueElements
from swh.loader.svn.queue import QueuePerSizeAndNbUniqueElements


class TestQueuePerNbElements(unittest.TestCase):
    @istest
    def simple_queue_behavior(self):
        max_nb_elements = 10
        queue = QueuePerNbElements(max_nb_elements=max_nb_elements)

        elements = [1, 3, 4, 9, 20, 30, 40]
        actual_threshold = queue.add(elements)

        self.assertFalse(actual_threshold, len(elements) > max_nb_elements)

        # pop returns the content and reset the queue
        actual_elements = queue.pop()
        self.assertEquals(actual_elements, elements)
        self.assertEquals(queue.pop(), [])

        # duplicates can be integrated
        new_elements = [1, 1, 3, 4, 9, 20, 30, 40, 12, 14, 2]
        actual_threshold = queue.add(new_elements)

        self.assertTrue(actual_threshold)
        self.assertEquals(queue.pop(), new_elements)

        # reset is destructive too
        queue.add(new_elements)
        queue.reset()

        self.assertEquals(queue.pop(), [])


def to_some_objects(elements, key):
    for elt in elements:
        yield {key: elt}


class TestQueuePerNbUniqueElements(unittest.TestCase):
    @istest
    def queue_with_unique_key_behavior(self):
        max_nb_elements = 5
        queue = QueuePerNbUniqueElements(max_nb_elements=max_nb_elements,
                                         key='id')

        # no duplicates
        elements = list(to_some_objects([1, 1, 3, 4, 9], key='id'))
        actual_threshold = queue.add(elements)

        self.assertFalse(actual_threshold, len(elements) > max_nb_elements)

        # pop returns the content and reset the queue
        actual_elements = queue.pop()
        self.assertEquals(actual_elements,
                          [{'id': 1}, {'id': 3}, {'id': 4}, {'id': 9}])
        self.assertEquals(queue.pop(), [])

        new_elements = list(to_some_objects(
            [1, 3, 4, 9, 20],
            key='id'))
        actual_threshold = queue.add(new_elements)

        self.assertTrue(actual_threshold)

        # reset is destructive too
        queue.add(new_elements)
        queue.reset()

        self.assertEquals(queue.pop(), [])


def to_some_complex_objects(elements, key):
    for elt, size in elements:
        yield {key: elt, 'length': size}


class TestQueuePerSizeAndNbUniqueElements(unittest.TestCase):
    @istest
    def queue_with_unique_key_and_size_behavior(self):
        max_nb_elements = 5
        max_size = 100
        queue = QueuePerSizeAndNbUniqueElements(
            max_nb_elements=max_nb_elements,
            max_size=max_size,
            key='k')

        # size total exceeded, nb elements not reached, still the
        # threshold is deemed reached
        elements = list(to_some_complex_objects([(1, 10),
                                                 (2, 20),
                                                 (3, 30),
                                                 (4, 100)], key='k'))
        actual_threshold = queue.add(elements)

        self.assertTrue(actual_threshold)

        # pop returns the content and reset the queue
        actual_elements = queue.pop()
        self.assertEquals(actual_elements,
                          [{'k': 1, 'length': 10},
                           {'k': 2, 'length': 20},
                           {'k': 3, 'length': 30},
                           {'k': 4, 'length': 100}])
        self.assertEquals(queue.pop(), [])

        # size threshold not reached, nb elements reached, the
        # threshold is considered reached
        new_elements = list(to_some_complex_objects(
            [(1, 10), (3, 5), (4, 2), (9, 1), (20, 0)],
            key='k'))
        actual_threshold = queue.add(new_elements)

        queue.reset()

        self.assertTrue(actual_threshold)

        # nb elements threshold not reached, nor the top number of
        # elements, the threshold is not reached
        new_elements = list(to_some_complex_objects(
            [(1, 10)],
            key='k'))
        actual_threshold = queue.add(new_elements)

        self.assertFalse(actual_threshold)

        # reset is destructive too
        queue.add(new_elements)
        queue.reset()

        self.assertEquals(queue.pop(), [])
