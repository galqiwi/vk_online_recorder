import os
import sys
import json
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tqdm
import zlib
import seaborn as sns
import datetime
from storage import Storage, Entry

root = f'data/'
days_dir = sorted(os.listdir(root))
dates = [datetime.date.fromisoformat(day) for day in days_dir]

storage = Storage(db_path='online_data.db', reset=True)

for day, date in zip(days_dir, dates):
    # if date < datetime.date(2020, 1, 26):
    #     continue
    day_full_path = os.path.join(root, day)
    print(day)
    for snapshot_name in sorted(os.listdir(day_full_path)):
        snapshot_time =\
            datetime.datetime.strptime(snapshot_name[:-len('.data')],
                                       '%Y-%m-%d-%H.%M.%S')
        minute_id = snapshot_time.hour * 60 + snapshot_time.minute

        with open(os.path.join(day_full_path, snapshot_name), 'r') as file:
            snapshot_raw_data = json.loads(file.read().split('\n')[1])
        data = \
            [Entry(user_id, True) for user_id in snapshot_raw_data['online']]+\
            [Entry(user_id, False) for user_id in snapshot_raw_data['offline']]

        storage.add(minute_id=minute_id, date=date, data=data)

