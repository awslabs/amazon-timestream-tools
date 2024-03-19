#!/usr/bin/env bash
#
# The MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

echo "Wait to start InfluxDB 2.0"
wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:8086/metrics

echo
echo "Post onBoarding request, to setup initial user (test-user@test-password), org (test-org) and bucketSetup (test-bucket)"
echo
curl -i -X POST http://localhost:8086/api/v2/setup -H 'accept: application/json' \
    -d '{
            "username": "test-user",
            "password": "test-password",
            "org": "test-org",
            "bucket": "test-bucket",
            "token": "test-token"
        }'

curl --request POST \
  http://localhost:8086/api/v2/query?org=test-org  \
  --header 'Authorization: Token test-token' \
  --header 'Accept: application/csv' \
  --header 'Content-type: application/vnd.flux' \
  --data 'import "influxdata/influxdb/sample"
        sample.data(set: "birdMigration")
            |> to(bucket: "test-bucket")'
