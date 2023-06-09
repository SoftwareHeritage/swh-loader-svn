# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.tests import assert_module_tasks_are_scheduler_ready


def test_tasks_loader_visit_type_match_task_name():
    import swh.loader.svn

    assert_module_tasks_are_scheduler_ready([swh.loader.svn])
