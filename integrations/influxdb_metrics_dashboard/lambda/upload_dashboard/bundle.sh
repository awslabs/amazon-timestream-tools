#!/bin/bash

GOOS=linux GOARCH=arm64 go build -o lambda/upload_dashboard/bootstrap lambda/upload_dashboard/main.go
zip lambda/upload_dashboard/lambda.zip lambda/upload_dashboard/bootstrap -j
rm lambda/upload_dashboard/bootstrap
