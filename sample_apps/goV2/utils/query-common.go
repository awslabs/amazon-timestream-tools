package utils

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
)

func fail(s string) {
	panic(s)
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func Write(f *os.File, s string) {
	if f != nil {
		_, err := f.WriteString(s + "\n")
		Check(err)
		f.Sync()
	}
}

func processScalarType(data *types.Datum) string {
	return *data.ScalarValue
}

func processTimeSeriesType(data []types.TimeSeriesDataPoint, columnInfo *types.ColumnInfo) string {
	value := ""
	for k := 0; k < len(data); k++ {
		time := data[k].Time
		value += *time + ":"
		if columnInfo.Type.ScalarType != "" {
			value += processScalarType(data[k].Value)
		} else if columnInfo.Type.ArrayColumnInfo != nil {
			value += processArrayType(data[k].Value.ArrayValue, columnInfo.Type.ArrayColumnInfo)
		} else if columnInfo.Type.RowColumnInfo != nil {
			value += processRowType(data[k].Value.RowValue.Data, columnInfo.Type.RowColumnInfo)
		} else {
			fail("Bad data type")
		}
		if k != len(data)-1 {
			value += ", "
		}
	}
	return value
}

func processArrayType(datumList []types.Datum, columnInfo *types.ColumnInfo) string {
	value := ""
	for k := 0; k < len(datumList); k++ {
		if columnInfo.Type.ScalarType != "" {
			value += processScalarType(&datumList[k])
		} else if columnInfo.Type.TimeSeriesMeasureValueColumnInfo != nil {
			value += processTimeSeriesType(datumList[k].TimeSeriesValue, columnInfo.Type.TimeSeriesMeasureValueColumnInfo)
		} else if columnInfo.Type.ArrayColumnInfo != nil {
			value += "["
			value += processArrayType(datumList[k].ArrayValue, columnInfo.Type.ArrayColumnInfo)
			value += "]"
		} else if columnInfo.Type.RowColumnInfo != nil {
			value += "["
			value += processRowType(datumList[k].RowValue.Data, columnInfo.Type.RowColumnInfo)
			value += "]"
		} else {
			fail("Bad column type")
		}

		if k != len(datumList)-1 {
			value += ", "
		}
	}
	return value
}

func processRowType(data []types.Datum, metadata []types.ColumnInfo) string {
	value := ""
	for j := 0; j < len(data); j++ {
		if metadata[j].Type.ScalarType != "" {
			// process simple data types
			value += processScalarType(&data[j])
		} else if metadata[j].Type.TimeSeriesMeasureValueColumnInfo != nil {
			datapointList := data[j].TimeSeriesValue
			value += "["
			value += processTimeSeriesType(datapointList, metadata[j].Type.TimeSeriesMeasureValueColumnInfo)
			value += "]"
		} else if metadata[j].Type.ArrayColumnInfo != nil {
			columnInfo := metadata[j].Type.ArrayColumnInfo
			datumList := data[j].ArrayValue
			value += "["
			value += processArrayType(datumList, columnInfo)
			value += "]"
		} else if metadata[j].Type.RowColumnInfo != nil {
			columnInfo := metadata[j].Type.RowColumnInfo
			datumList := data[j].RowValue.Data
			value += "["
			value += processRowType(datumList, columnInfo)
			value += "]"
		} else {
			panic("Bad column type")
		}
		// comma seperated column values
		if j != len(data)-1 {
			value += ", "
		}
	}
	return value
}

func RunQuery(queryPtr *string, querySvc *timestreamquery.Client, f *os.File, maxRows int32) error {
	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String(*queryPtr),
		MaxRows:     aws.Int32(maxRows),
	}

	for {
		queryResponse, err := querySvc.Query(context.TODO(), queryInput)
		if err != nil {
			fmt.Printf("Error while querying the query %s : %s\n", *queryPtr, err.Error())
			return err
		}

		ParseQueryResult(queryResponse, f)
		msg := fmt.Sprintf("Number of Rows: %d\n", len(queryResponse.Rows))
		fmt.Print(msg)
		Write(f, msg)

		if queryResponse.NextToken == nil {
			break
		}
		queryInput.NextToken = queryResponse.NextToken
		queryResponse, _ = querySvc.Query(context.TODO(), queryInput)
	}
		return nil
}
