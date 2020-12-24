import time
import random
import math
import uuid
import sys

from influxdb import InfluxDBClient
from influxdb import SeriesHelper

if len(sys.argv) != 3:
    print('Usage: {0} influxdb_host influxdb_port'.format(sys.argv[0]))
    print('Invoked: {0}'.format(sys.argv))
    sys.exit(1)

# InfluxDB connections settings
host = sys.argv[1]
port = int(sys.argv[2])
user = ''
password = ''
dbname = ''

myclient = InfluxDBClient(host, port, user, password, dbname)

class MySeriesHelper(SeriesHelper):
    """Instantiate SeriesHelper to write points to the backend."""

    class Meta:
        """Meta class stores time series helper configuration."""

        # The client should be an instance of InfluxDBClient.
        client = myclient

        series_name = 'sample_app'

        # Defines all the fields in this time series.
        fields = ['pi', 'iteration']

        # Defines all the tags for the series.
        tags = ['session_uuid']

        # Defines the number of data points to store prior to writing
        # on the wire.
        bulk_size = 5

        # autocommit must be set to True when using bulk_size
        autocommit = True


session_uuid = str(uuid.uuid4())
inside = 0
total = 0

while True:
  total += 1

  # Generate random x, y in [0, 1].
  x2 = random.random()**2
  y2 = random.random()**2
  # Increment if inside unit circle.
  if math.sqrt(x2 + y2) < 1.0:
    inside += 1

  # inside / total = pi / 4
  pi = (float(inside) / total) * 4
  MySeriesHelper(session_uuid=session_uuid, pi=pi, iteration=total)
  # To manually submit data points which are not yet written, call commit:
  MySeriesHelper.commit()
  print('Sent to InfluxDB iteration: {}, pi: {}. Sleeping 1s'.format(total, pi))
  time.sleep(1)
