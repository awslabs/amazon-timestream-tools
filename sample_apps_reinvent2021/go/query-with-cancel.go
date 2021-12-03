package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"go_sample/utils"

	"flag"
	"fmt"
	"os"
)

func main() {
	// process command line arguments
	region := flag.String("region", "us-east-1", "region")
	queryPtr := flag.String("query", "", "query string")
	filePtr := flag.String("outputfile", "", "output results file in the current folder")
	flag.Parse()

	// setup the query client
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	querySvc := timestreamquery.New(sess)

	var f *os.File
	if *filePtr != "" {
		var ferr error
		f, ferr = os.Create(*filePtr)
		utils.Check(ferr)
		defer f.Close()
	}

	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String(*queryPtr),
	}

	fmt.Println("Submitting a query:")
	fmt.Println(queryInput)
	// submit the query
	queryOutput, err := querySvc.Query(queryInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	}

	cancelQueryInput := &timestreamquery.CancelQueryInput{
		QueryId: aws.String(*queryOutput.QueryId),
	}

	fmt.Println("Submitting cancellation for the query")
	fmt.Println(cancelQueryInput)

	// submit the query
	cancelQueryOutput, err := querySvc.CancelQuery(cancelQueryInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Query has been cancelled successfully")
		fmt.Println(cancelQueryOutput)
	}

}
