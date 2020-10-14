package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"

	"flag"
	"fmt"
	"os"
)

func main() {
	// setup the query client
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
		return
	}

	// querySvc := timestreamquery.New(sess, &aws.Config{Endpoint: aws.String("query-cell0.timestream.us-east-1.amazonaws.com")})
	querySvc := timestreamquery.New(sess)

	// process command line arguments
	queryPtr := flag.String("query", "", "query string")
	filePtr := flag.String("outputfile", "", "output results file in the current folder")
	flag.Parse()
	var f *os.File
	if *filePtr != "" {
		var ferr error
		f, ferr = os.Create(*filePtr)
		check(ferr)
		defer f.Close()
	}

	runQuery(queryPtr, querySvc, f)
}
