import unittest

from python_test_task.main import Category, build_category_tree


class TestMain(unittest.TestCase):
    def test_build_category_tree(self):
        categories = dict()
        categories["1"] = {"parent_id": "", "name": "Детям", "path": ()}
        categories["3"] = {"parent_id": "2", "name": "Куклы", "path": ()}
        categories["2"] = {"parent_id": "1", "name": "Девочкам", "path": ()}
        answer = {
            Category("1", "", "Детям", ()),
            Category("2", "1", "Девочкам", ("1",)),
            Category("3", "2", "Куклы", ("1", "2"))
        }

        categories = build_category_tree(categories)

        assert categories == answer
