import pathlib
import sys

sys.path.append(str((pathlib.Path.cwd() / "src").resolve()))

import unittest

from python_test_task.main import Category, build_category_tree


class TestMain(unittest.TestCase):
    def test_build_category_tree(self):
        categories = dict()
        categories["1"] = {
            "id": "1",
            "parent_id": "",
            "name": "Детям",
            "path": ("Детям",),
        }
        categories["3"] = {
            "id": "3",
            "parent_id": "2",
            "name": "Куклы",
            "path": ("Куклы",),
        }
        categories["2"] = {
            "id": "2",
            "parent_id": "1",
            "name": "Девочкам",
            "path": ("Девочкам",),
        }
        answer = {
            "1": Category("1", "", "Детям", "Детям"),
            "2": Category("2", "1", "Девочкам", "Детям/Девочкам"),
            "3": Category("3", "2", "Куклы", "Детям/Девочкам/Куклы"),
        }

        categories = build_category_tree(categories)

        assert categories == answer
