package main

import (
  	"bytes"
  	"context"
  	"crypto/sha256"
  	"encoding/hex"
    "flag"
  	"fmt"
  	"io"
  	"net/http"
  	"time"
    "os"
    "bufio"
    
    influxdbhttp "github.com/influxdata/influxdb-client-go/v2/api/http"
    "github.com/influxdata/influxdb-client-go/v2"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// UserAgentSetter is the implementation of Doer interface for setting the SigV4 headers
type SigV4HeaderSetter struct {
    RequestDoer influxdbhttp.Doer
}

var(
    region string
    service string
    endpoint string
    dataset string
    precision string
    useGzip bool
)

func main() {
    flag.StringVar(&region, "region", "us-east-1", "AWS region for InfluxDB Timestream Connector")
    flag.StringVar(&service, "service", "lambda", "Service value for SigV4 header")
    flag.StringVar(&endpoint, "endpoint", "http://localhost:9000", "Endpoint for InfluxDB Timestream Connector")
    flag.StringVar(&dataset, "dataset", "../data/bird-migration.line", "Line protocol dataset being ingested")
    flag.StringVar(&precision, "precision", "ns", "Precision for line protocol: nanoseconds=ns, milliseconds=ms, microseconds=us, seconds=s")
    flag.BoolVar(&useGzip, "gzip", false, "Whether to compress requests with gzip")
    flag.Parse()

    opts := influxdb2.DefaultOptions()
    opts.HTTPOptions().SetHTTPDoer(&SigV4HeaderSetter{RequestDoer: opts.HTTPClient(),})
    opts.WriteOptions().SetUseGZip(useGzip)

    switch {
    case precision == "ns":
        opts.WriteOptions().SetPrecision(time.Nanosecond)
    case precision == "ms":
        opts.WriteOptions().SetPrecision(time.Millisecond)
    case precision == "us":
        opts.WriteOptions().SetPrecision(time.Microsecond)
    case precision == "s":
        opts.WriteOptions().SetPrecision(time.Second)
    default:
        fmt.Println("Invalid precision value, valid values include: nanoseconds=ns, milliseconds=ms, microseconds=us, seconds=s")
        return
    }

    bucket := ""
    org := ""
    token := ""

    file, err := os.Open(dataset)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()

    sc := bufio.NewScanner(file)
    lines := make([]string, 0)

    for sc.Scan() {
        lines = append(lines, sc.Text())
    }

    if err := sc.Err(); err != nil {
        fmt.Println(err)
        return
    }

    client := influxdb2.NewClientWithOptions(endpoint, token, opts)
    writeAPI := client.WriteAPIBlocking(org, bucket)
    err = writeAPI.WriteRecord(context.Background(), lines[0:]...)
    if err != nil {
        panic(err)
    }

    // Ensures background processes finishes
    client.Close()
}

// Do is called before each request is made
func (u *SigV4HeaderSetter) Do(req *http.Request) (*http.Response, error) {
    ctx := context.Background()
    signer := v4.NewSigner()

    cfg, _ := config.LoadDefaultConfig(context.TODO())
    credentialsValue, _ := cfg.Credentials.Retrieve(context.TODO())

    bodyBytes, _ := io.ReadAll(req.Body)
    reader1 := io.NopCloser(bytes.NewBuffer(bodyBytes))
    reader2 := io.NopCloser(bytes.NewBuffer(bodyBytes))
    bodyBytes, _ = io.ReadAll(reader1)

    hash := sha256.New()
    hash.Write([]byte(string(bodyBytes)))
    hashedAndEncodedBody := hex.EncodeToString(hash.Sum(nil))

    signer.SignHTTP(ctx, credentialsValue, req, hashedAndEncodedBody, service, region, time.Now())
    req.Body = reader2

    // Call original Doer to proceed with request
    return u.RequestDoer.Do(req)
}

