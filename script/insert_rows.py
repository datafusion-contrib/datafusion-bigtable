# cbt does not support setting integer value
# so use Python SDK
from google.auth.credentials import AnonymousCredentials
from google.cloud._helpers import _datetime_from_microseconds
from google.cloud import bigtable
from google.cloud.bigtable.table import DEFAULT_RETRY
from google.cloud.bigtable.row import DirectRow

data_tuples = [
    ("us-west2#3698#2021-03-05-1200", 94558, "9.6", 1614945605100000),
    ("us-west2#3698#2021-03-05-1201", 94122, "9.7", 1614945665200000),
    ("us-west2#3698#2021-03-05-1202", 95992, "9.5", 1614945725300000),
    ("us-west2#3698#2021-03-05-1203", 96025, "9.5", 1614945785400000),
    ("us-west2#3698#2021-03-05-1204", 96021, "9.6", 1614945845500000),
]

PROJECT_ID = "emulator"
INSTANCE_ID = "dev"
TABLE_NAME = "weather_balloons"
COLUMN_FAMILY_ID = "measurements"
SUCCESS = 0

client = bigtable.client.Client(project=PROJECT_ID, credentials=AnonymousCredentials())
bigtable_table = client.instance(INSTANCE_ID).table(TABLE_NAME)

rows = []
for (row_key, pressure, temperature, timestamp_microseconds) in data_tuples:
    row = DirectRow(row_key.encode())
    # integer value
    row.set_cell(COLUMN_FAMILY_ID, "pressure".encode(), pressure, timestamp=_datetime_from_microseconds(timestamp_microseconds))
    # string value
    row.set_cell(COLUMN_FAMILY_ID, "temperature".encode(), temperature.encode(), timestamp=_datetime_from_microseconds(timestamp_microseconds))
    rows.append(row)

# no retry
statues = bigtable_table.mutate_rows(rows, timeout=5, retry=DEFAULT_RETRY.with_deadline(5))
print([(s.code == SUCCESS, s) for s in statues])
