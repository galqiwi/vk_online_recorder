from storage.bits2bytes import bits2bytes, bytes2bits
import numpy as np
from storage.db_wrapper import Database
from dataclasses import dataclass
import zlib


@dataclass(frozen=False)
class Entry:
    user_id: int
    online: bool


class Storage:
    def __init__(self, db_path='storage.db', reset=False):
        self.database = Database(db_path=db_path, reset=reset)

    @staticmethod
    def _convert_date_to_hot_snapshot_table_name(date):
        return f'hot_{date.strftime("%Y_%m_%d")}'

    @staticmethod
    def _convert_date_to_hot_users_table_name(date):
        return f'hot_users_{date.strftime("%Y_%m_%d")}'

    def has_date(self, date):
        snapshot_table = self._convert_date_to_hot_snapshot_table_name(date)
        return self.database.table_exists(f'{snapshot_table}')

    def _initialise_hot_tables_if_needed(self, date):
        snapshot_table = self._convert_date_to_hot_snapshot_table_name(date)
        user_table = self._convert_date_to_hot_users_table_name(date)
        self.database.execute(
            f'create table if not exists '
            f'{snapshot_table}(minute_id INT, snapshot BLOB)'
        )
        self.database.execute(
            f'create table if not exists '
            f'{user_table}(user_id INT)'
        )

    def _get_all_users_available_in_hot_date(self, date):
        user_table = self._convert_date_to_hot_users_table_name(date)
        return [
            _[0] for _ in self.database.execute(
                f'select user_id from {user_table}'
            ).response]

    def _add_users_to_hot_day_if_needed(self, date, users):
        user_table = self._convert_date_to_hot_users_table_name(date)
        all_users_set = set(self._get_all_users_available_in_hot_date(date))
        self.database.executemany(
            f'insert into {user_table} values (?)',
            [(user_id,) for user_id in users if user_id not in all_users_set])

    def _get_hot_day_snapshot(self, date, data):
        self._add_users_to_hot_day_if_needed(date,
                                             [entry.user_id for entry in data])
        all_users = self._get_all_users_available_in_hot_date(date)

        data_dict = {entry.user_id: entry.online for entry in data}
        snapshot = np.array(
            [data_dict.get(user, False) for user in all_users], dtype=bool
        )
        return self._compress_snapshot(snapshot)

    @staticmethod
    def _decompress_snapshot(snapshot):
        return bytes2bits(
            np.array(list(zlib.decompress(snapshot)), dtype=np.ubyte))

    @staticmethod
    def _compress_snapshot(snapshot):
        return zlib.compress(bits2bytes(snapshot))

    def _get_all_snapshots_by_hot_date(self, date):
        snapshot_table = self._convert_date_to_hot_snapshot_table_name(date)
        return self.database.execute(
            f'select * from {snapshot_table}').response

    def _get_maximum_minute_id_on_hot_date(self, date, default=-1):
        snapshot_table = self._convert_date_to_hot_snapshot_table_name(date)
        max_minute = self.database.execute(
            f'select max(minute_id) from {snapshot_table}').response[0][0]
        return max_minute if max_minute is not None else default

    class MinuteIdIsNotIncreasing(Exception):
        pass

    def add(self, minute_id, date, data):
        snapshot_table = self._convert_date_to_hot_snapshot_table_name(date)
        self._initialise_hot_tables_if_needed(date)
        snapshot = self._get_hot_day_snapshot(date, data)

        if minute_id >= 24 * 60:
            raise ValueError(f'{minute_id = } is not valid')

        if minute_id <= self._get_maximum_minute_id_on_hot_date(date):
            raise self.MinuteIdIsNotIncreasing()

        self.database.execute(
            f'insert into {snapshot_table} values (?, ?)',
            (minute_id, snapshot)
        )

    def load(self, date, uid):
        output = np.zeros(shape=(60 * 24,), dtype=bool)
        if not self.has_date(date):
            return output

        all_users = self._get_all_users_available_in_hot_date(date)

        if uid not in all_users:
            return output

        user_index = all_users.index(uid)

        snapshots = self._get_all_snapshots_by_hot_date(date)
        for minute_id, snapshot in snapshots:
            snapshot_bits = self._decompress_snapshot(snapshot)
            if user_index < len(snapshot_bits):
                output[minute_id] = snapshot_bits[user_index]

        return output
