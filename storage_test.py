import unittest
import numpy as np
from storage import Storage, Entry


class StorageTests(unittest.TestCase):
    def test_new_storage_has_no_date(self):
        storage = Storage(reset=True, db_path='test.db')
        self.assertFalse(storage.has_date('10_10_2001'))

    def test_storage_has_date_after_adding(self):
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        self.assertTrue(storage.has_date('10_10_2001'))

    def test_storage_is_persistent(self):
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        storage2 = Storage(reset=False, db_path='test.db')
        self.assertTrue(storage2.has_date('10_10_2001'))

    def test_empty_storage_resets(self):
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        self.assertTrue(storage.has_date('10_10_2001'))
        storage = Storage(reset=True, db_path='test.db')
        self.assertFalse(storage.has_date('10_10_2001'))

    @staticmethod
    def test_empty_storage_returns_empty_load():
        storage = Storage(reset=True, db_path='test.db')
        output = storage.load(date='10_10_2001', uid=0)
        desired_output = np.zeros(shape=(60 * 24,), dtype=bool)
        np.testing.assert_array_equal(output, desired_output)

    def test_storage_return_type(self):
        storage = Storage(reset=True, db_path='test.db')
        output = storage.load(date='10_10_2001', uid=0)
        self.assertEqual(output.dtype, bool)

    @staticmethod
    def test_storage_returns_what_was_added():
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        output = storage.load(date='10_10_2001', uid=0)
        desired_output = np.zeros(shape=(60 * 24,), dtype=bool)
        desired_output[0] = True
        np.testing.assert_array_equal(output, desired_output)

    @staticmethod
    def test_storage_two_users():
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        storage.add(minute_id=1, date='10_10_2001', data=[Entry(1, True)])
        output_0 = storage.load(date='10_10_2001', uid=0)
        output_1 = storage.load(date='10_10_2001', uid=1)
        desired_output_0 = np.zeros(shape=(60 * 24,), dtype=bool)
        desired_output_0[0] = True
        desired_output_1 = np.zeros(shape=(60 * 24,), dtype=bool)
        desired_output_1[1] = True
        np.testing.assert_array_equal(output_0, desired_output_0)
        np.testing.assert_array_equal(output_1, desired_output_1)

    def test_minute_id_can_only_grow(self):
        storage = Storage(reset=True, db_path='test.db')
        storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, True)])
        with self.assertRaises(Storage.MinuteIdIsNotIncreasing):
            storage.add(minute_id=0, date='10_10_2001', data=[Entry(0, False)])

    def test_invalid_minute_id(self):
        storage = Storage(reset=True, db_path='test.db')
        with self.assertRaises(ValueError):
            storage.add(minute_id=60 * 24, date='10_10_2001',
                        data=[Entry(0, False)])


if __name__ == '__main__':
    unittest.main()
