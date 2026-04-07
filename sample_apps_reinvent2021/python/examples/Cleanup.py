#!/usr/bin/python

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from utils.WriteUtil import WriteUtil


class Cleanup:
    def __init__(self, database_name, table_name, write_client):
        self.database_name = database_name
        self.table_name = table_name
        self.write_client = write_client
        self.write_util = WriteUtil(write_client)

    def run(self):
        self.write_util.delete_table(self.database_name, self.table_name)
        self.write_util.delete_database(self.database_name)
