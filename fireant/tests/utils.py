from unittest import TestCase

from fireant import utils


class MergeDictTests(TestCase):
    def setUp(self):
        self.dict1 = {'first_name': 'John', 'gender': 'Male'}
        self.dict2 = {'last_name': 'Smith'}
        self.dict3 = {'age': 50}

    def test_merge_dict_does_not_modify_dicts(self):
        utils.merge_dicts(self.dict1, self.dict2, self.dict3)

        self.assertEqual(self.dict1, {'first_name': 'John', 'gender': 'Male'})
        self.assertEqual(self.dict2, {'last_name': 'Smith'})
        self.assertEqual(self.dict3, {'age': 50})

    def test_merge_dict_merges_dicts_successfully(self):
        result = utils.merge_dicts(self.dict1, self.dict2, self.dict3)

        self.assertEqual(result, {
            'first_name': 'John',
            'gender': 'Male',
            'last_name': 'Smith',
            'age': 50
        })

    def test_dict_on_right_side_overwrites_left_side_dict(self):
        mary_dict = {'first_name': 'Mary', 'gender': 'Female'}
        result = utils.merge_dicts(self.dict1, mary_dict)
        self.assertEqual(result, mary_dict)
