# Database for tracking online of vk.com users

## Usage
``` python
from storage import Storage, Entry

storage = Storage(db_path='test.db', reset=True)
storage.add(minute_id = 0, date = datetime.date.today(), data=[Entry(1, True)])  # vk.com/id1 is online
```