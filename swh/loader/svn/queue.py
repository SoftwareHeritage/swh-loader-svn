# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class QueuePerSize():
    """Data structure to add elements and count the current size of the queue.

    """
    def __init__(self, max_nb_elements, max_size, key):
        self.reset()
        self.max_nb_elements = max_nb_elements
        self.max_size = max_size
        self.key = key
        self.keys = set()

    def _add_element(self, e):
        k = e[self.key]
        if k not in self.keys:
            self.keys.add(k)
            self.elements.append(e)
            self.size += e['length']
            self.count += 1

    def add(self, elements):
        for e in elements:
            self._add_element(e)
        return self.size >= self.max_size or \
            self.count >= self.max_nb_elements

    def size(self):
        return self.size

    def pop(self):
        elements = self.elements
        self.reset()
        return elements

    def reset(self):
        self.elements = []
        self.keys = set()
        self.size = 0
        self.count = 0


class QueuePerNbElements():
    """Data structure to hold elements and the actual counts on it.

    """
    def __init__(self, max_nb_elements, key):
        self.reset()
        self.max_nb_elements = max_nb_elements
        self.key = key
        self.keys = set()

    def _add_element(self, e):
        k = e[self.key]
        if k not in self.keys:
            self.keys.add(k)
            self.elements.append(e)
            self.count += 1

    def add(self, elements):
        for e in elements:
            self._add_element(e)
        return self.count >= self.max_nb_elements

    def size(self):
        return self.count

    def pop(self):
        elements = self.elements
        self.reset()
        return elements

    def reset(self):
        self.elements = []
        self.keys = set()
        self.count = 0
