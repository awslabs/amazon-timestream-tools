##################################################
## A helper to clean up the Timestream database ##
## and table used for the performance run ########
##################################################

import timestreamwrite as tswrite
import argparse
import pprint

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog = 'DevOps Cleanup', description='Cleanup resources for the DevOps workload.')

    parser.add_argument('--database-name', '-d', dest="databaseName", action = "store", required = True, help = "The database name for the workload.")
    parser.add_argument('--table-name', '-t', dest="tableName", action = "store", required = True, help = "The table name for the workload.")
    parser.add_argument('--region', '-r', action = "store", required = True, help="Specify the region where the Timestream database is located.")
    parser.add_argument('--endpoint', '-e', action = "store", default = None, help="Specify the endpoint where the Timestream database is located.")
    parser.add_argument('--profile', action = "store", type = str, default= None, help = "The AWS profile to use.")

    args = parser.parse_args()
    print(args)

    client = tswrite.createWriteClient(region=args.region, profile=args.profile, endpoint=args.endpoint)
    try:
        result = tswrite.deleteTable(client, args.databaseName, args.tableName)
        print("Delete table status: ", result['ResponseMetadata']['HTTPStatusCode'])

        result = tswrite.deleteDatabase(client, args.databaseName)
        print("Delete database status: ", result['ResponseMetadata']['HTTPStatusCode'])
    except client.exceptions.ResourceNotFoundException as e:
        pprint.pprint(e)
        raise e
    except Exception as e:
        pprint.pprint(e)
        raise e