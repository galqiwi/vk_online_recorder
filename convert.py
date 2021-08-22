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
from storage import Storage, Entry

root = 'test-unzipped/2020-05-02'
files = sorted(os.listdir(root))
assert len(files) == 60 * 24

storage = Storage(db_path='day.db', reset=True)

for snapshot_id, snapshot_name in tqdm.tqdm(enumerate(files)):
    with open(os.path.join(root, snapshot_name), 'r') as file:
        snapshot_data = json.loads(file.read().split('\n')[1])
    data = [Entry(user_id, True) for user_id in snapshot_data['online']] + \
        [Entry(user_id, False) for user_id in snapshot_data['offline']]
    storage.add(minute_id=snapshot_id, date='2020_05_02', data=data)
