import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# Create a table with nanosecond precision timestamps
data = [
    pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    pa.array([
        datetime(2023, 1, 1, 10, 0, 0),
        datetime(2023, 1, 1, 10, 0, 1),
        datetime(2023, 1, 1, 10, 0, 2),
        datetime(2023, 1, 1, 10, 0, 3),
        datetime(2023, 1, 1, 10, 0, 4)
    ], type=pa.timestamp('ns'))  # nanosecond precision
]

schema = pa.schema([
    ('id', pa.int32()),
    ('timestamp_nanos', pa.timestamp('ns'))
])

table = pa.table(data, schema=schema)
pq.write_table(table, 'test_nanos_timestamp.parquet')

print("Parquet file created!")
print("Schema:", table.schema)
