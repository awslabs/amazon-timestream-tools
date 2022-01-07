package main

import (
	"go_sample/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"

	"context"
	"flag"
	"fmt"
	"os"
)

func main() {
	// process command line arguments
	region := flag.String("region", utils.REGION, "region")
	queryPtr := flag.String("query", "", "query string")
	filePtr := flag.String("outputfile", "", "output results file in the current folder")
	flag.Parse()

	// setup the query client
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*region))
	querySvc := timestreamquery.NewFromConfig(cfg, func(o *timestreamquery.Options) {
		o.Region = *region
	})

	if *queryPtr == "" {
		fmt.Println("Invalid Input: --query must be specified to non empty")
		os.Exit(-1)
	}

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
	fmt.Println(*queryPtr)
	// submit the query
	queryOutput, err := querySvc.Query(context.TODO(), queryInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	}

	cancelQueryInput := &timestreamquery.CancelQueryInput{
		QueryId: aws.String(*queryOutput.QueryId),
	}

	fmt.Println("Submitting cancellation for the query")
	fmt.Println(utils.JsonMarshalIgnoreError(cancelQueryInput))

	// submit the query
	cancelQueryOutput, err := querySvc.CancelQuery(context.TODO(), cancelQueryInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Query has been cancelled successfully")
		fmt.Println(utils.JsonMarshalIgnoreError(cancelQueryOutput))
	}

}
