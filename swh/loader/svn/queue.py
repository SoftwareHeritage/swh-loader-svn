# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class QueuePerNbElements():
    """Basic queue which holds the nb of elements it contains.

    """
    def __init__(self, max_nb_elements):
        self.reset()
        self.max_nb_elements = max_nb_elements

    def add(self, elements):
        if not isinstance(elements, list):
            elements = list(elements)
        self.elements.extend(elements)
        self.count += len(elements)
        return self.count >= self.max_nb_elements

    def pop(self):
        elements = self.elements
        self.reset()
        return elements

    def reset(self):
        self.elements = []
        self.count = 0


class QueuePerSizeAndNbUniqueElements():
    """Queue which permits to add unknown elements and holds the current
       size of the queue.

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

    def pop(self):
        elements = self.elements
        self.reset()
        return elements

    def reset(self):
        self.elements = []
        self.keys = set()
        self.size = 0
        self.count = 0


class QueuePerNbUniqueElements():
    """Queue which permits to add unknown elements and knows the actual
       count of elements it held.

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

    def pop(self):
        elements = self.elements
        self.reset()
        return elements

    def reset(self):
        self.elements = []
        self.keys = set()
        self.count = 0
