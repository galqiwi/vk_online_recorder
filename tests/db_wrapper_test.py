import unittest
from storage.storage import Database


class DbWrapperTests(unittest.TestCase):
    def test_table_exists(self):
        database = Database(db_path='../test.db', reset=True)
        self.assertFalse(database.table_exists('table_name'))
        database.execute('create table table_name(field int)')
        self.assertTrue(database.table_exists('table_name'))

    def test_table_persistent(self):
        database = Database(db_path='../test.db', reset=True)
        database.execute('create table table_name(field int)')
        database2 = Database(db_path='../test.db')
        self.assertTrue(database2.table_exists('table_name'))

    def test_table_resets(self):
        database = Database(db_path='../test.db', reset=True)
        database.execute('create table table_name(field int)')
        database2 = Database(db_path='../test.db', reset=True)
        self.assertFalse(database2.table_exists('table_name'))


if __name__ == '__main__':
    unittest.main()
