import os
import sqlite3
from typing import List
from dataclasses import dataclass


@dataclass(frozen=True)
class Response:
    response: List[List[object]]
    last_row_id: int

    def __len__(self):
        return len(self.response)

    def is_empty(self):
        return len(self) == 0


class Database:
    def __init__(self, db_path, reset=False):
        self.db_path = db_path
        if reset and os.path.isfile(db_path):
            os.remove(db_path)

    def execute(self, *args, **kwargs):
        with sqlite3.connect(self.db_path) as con:
            cur = con.execute(*args, **kwargs)
            last_row_id = cur.lastrowid
            response = cur.fetchall()
            return Response(response, last_row_id)

    def executemany(self, *args, **kwargs):
        with sqlite3.connect(self.db_path) as con:
            con.executemany(*args, **kwargs)

    def table_exists(self, table_name):
        return not self.execute('SELECT name FROM sqlite_master '
                                'WHERE type="table" AND name=?',
                                (table_name,)).is_empty()
