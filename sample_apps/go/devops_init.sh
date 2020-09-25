#!/bin/sh

if test $# -ne 2; then
    echo "Usage:"
    echo "./$2.sh <database name> <table name>"
    echo "e.g. ./$2.sh DATABASE_NAME TABLE_NAME"
    exit -1
fi

# find the host with the maximum data points
go run query-common.go query-sample.go -query "SELECT hostname, count(*) as hostcount
FROM $1.$2
WHERE measure_name = 'cpu_utilization'
    AND time > ago(2h)
GROUP BY hostname
ORDER BY count(*) DESC"
 
