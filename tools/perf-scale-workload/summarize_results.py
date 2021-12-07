##################################################
## A helper to summarize the statistics from a  ##
## concurrent run for the query workload. ########
##################################################

import argparse
import pprint
import glob
import os
import csv
import numpy as np

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog = 'Summarize query latency', description='Summarize the runs from the multiple files emitted from a concurrent run.')

    parser.add_argument('--directory', '-d', dest="directory", action = "store", required = True, help = "The directory which has the runs for the experiment being summarized.")
    parser.add_argument('--pattern', '-p', dest="pattern", action = "store", default = "*.csv", help = "Pattern to search for in the files.")
    parser.add_argument('--output-file', '-o', dest="outputFile", action = "store", required = True, help = "The output file to store summarized output.")

    args = parser.parse_args()
    print(args)

    if not os.path.isdir(args.directory):
        print("Invalid path: ", args.directory)
        exit()

    headerString = None
    perQueryStats = dict()
    perQueryAggregatedStats = dict()
    try:
        ## Convert to absolute path. Add a trailing separator if it doesn't exist
        absoluteDirPath = os.path.join(os.path.abspath(args.directory), '')
        fileCount = 0
        ## Recursively read in add the files that contain the per thread summary based on the specified file pattern.
        for filename in glob.iglob(absoluteDirPath + os.path.join('**', args.pattern), recursive=True):
            with open(filename) as csvfile:
                lineReader = csv.reader(csvfile)
                header = True
                ## Here we assume that these files are comma-separated summary of the individual thread's execution stats.
                ## Each file has a header and number of rows of summarized stats.
                ## Each row has the first column as the name of the query, and subsequent columns as the stats.
                ## Example file with header and one row.
                ## Query type, Total Count, Successful Count, Avg. latency (in secs), Std dev latency (in secs), Median, 90th perc (in secs), 99th Perc (in secs), Geo Mean (in secs)
                ## do-q1,11586.5,11586.5,0.47,0.156,0.451,0.525,0.729,0.463
                for row in lineReader:
                    if header:
                        ## We assume all files have the same header row. So save the row of one of the files.
                        headerString = row
                        header = False
                        continue

                    ## The first column is the name of the query. We find all the summarizes for this operation across all the files.
                    if row[0] not in perQueryStats:
                        perQueryStats[row[0]] = list()
                    ## Store the stats second column onwards.
                    perQueryStats[row[0]].append([float(x) for x in row[1:]])
            fileCount += 1

        for key in perQueryStats.keys():
            value = perQueryStats[key]
            ## For each column, compute the average across all the files.
            perElementAvg = np.average(value, axis=0)
            perQueryAggregatedStats[key] = ["{0}".format(round(x, 3)) for x in perElementAvg]

        ## Write the summary in the aggregate file in the same format as the original files
        ## First row is the header read from the files.
        ## Second row onwards is the aggregated summary for each operation, averaged across all the files found in the specified path.
        outputFile = os.path.join(args.directory, args.outputFile)
        print("Processed {0} files. Summary written to: {1}".format(fileCount, outputFile))
        with open(outputFile, 'w') as out:
            writer = csv.writer(out)
            writer.writerow(headerString)
            for key in perQueryAggregatedStats.keys():
                row = list()
                row.append(key)
                row.extend(perQueryAggregatedStats[key])
                writer.writerow(row)

    except Exception as e:
        pprint.pprint(e)
        raise e