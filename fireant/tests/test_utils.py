from unittest import TestCase

from fireant.utils import write_named_temp_csv, read_csv


class TestFileOperations(TestCase):
    def test_write_named_temp_csv(self):
        data = [["a", 1, True, None, 1.8], ("", -1, False), ()]

        ntf = write_named_temp_csv(data)
        rows = read_csv(ntf.name)

        self.assertEqual(["a", "1", "True", "", "1.8"], rows[0])
        self.assertEqual(["", "-1", "False"], rows[1])
        self.assertEqual([], rows[2])
