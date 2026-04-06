// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanaTypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
	influxDBTypes "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
)

// Each InfluxDB instance info is set as environment variable in the format instance-name:instance-size:instance-storage-type
type influxDBClusterInfoIndex int

const (
	InstanceNameIdx influxDBClusterInfoIndex = iota
	InstanceSizeIdx
	InstanceStorageTypeIdx
)

// Struct for specifications related to InfluxDB instance size
type influxDBInstanceSpecs struct {
	vCpu                         int
	memory                       int
	networkBandwidth             int
	seriesThreshold              int
	lineWritesPerSecondThreshold int
	queriesPerSecondThreshold    int
}

// Struct for instance specific info
type influxDBInstanceInfo struct {
	instanceName        string
	instanceSize        influxDBTypes.DbInstanceType
	instanceStorageType influxDBTypes.DbStorageType
}

// Estimates based off developer documentation: https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html#timestream-for-influx-dbi-classt-hw
// Additional factors are not included for IOPS with options: InfluxIOIncludedT1, InfluxIOIncludedT2, InfluxIOIncludedT3
var influxDBInstanceTypes = map[influxDBTypes.DbInstanceType]influxDBInstanceSpecs{
	influxDBTypes.DbInstanceTypeDbInfluxMedium:   influxDBInstanceSpecs{vCpu: 1, memory: 8589934592, networkBandwidth: 1250000000, seriesThreshold: 10000, lineWritesPerSecondThreshold: 5000, queriesPerSecondThreshold: 5},
	influxDBTypes.DbInstanceTypeDbInfluxLarge:    influxDBInstanceSpecs{vCpu: 2, memory: 17179869184, networkBandwidth: 1250000000, seriesThreshold: 100000, lineWritesPerSecondThreshold: 50000, queriesPerSecondThreshold: 10},
	influxDBTypes.DbInstanceTypeDbInfluxXlarge:   influxDBInstanceSpecs{vCpu: 4, memory: 34359738368, networkBandwidth: 1250000000, seriesThreshold: 500000, lineWritesPerSecondThreshold: 100000, queriesPerSecondThreshold: 15},
	influxDBTypes.DbInstanceTypeDbInflux2xlarge:  influxDBInstanceSpecs{vCpu: 8, memory: 68719476736, networkBandwidth: 1250000000, seriesThreshold: 1000000, lineWritesPerSecondThreshold: 150000, queriesPerSecondThreshold: 25},
	influxDBTypes.DbInstanceTypeDbInflux4xlarge:  influxDBInstanceSpecs{vCpu: 16, memory: 137438953472, networkBandwidth: 1250000000, seriesThreshold: 5000000, lineWritesPerSecondThreshold: 250000, queriesPerSecondThreshold: 35},
	influxDBTypes.DbInstanceTypeDbInflux8xlarge:  influxDBInstanceSpecs{vCpu: 32, memory: 274877906944, networkBandwidth: 1500000000, seriesThreshold: 7500000, lineWritesPerSecondThreshold: 500000, queriesPerSecondThreshold: 50},
	influxDBTypes.DbInstanceTypeDbInflux12xlarge: influxDBInstanceSpecs{vCpu: 48, memory: 412316860416, networkBandwidth: 2500000000, seriesThreshold: 10000000, lineWritesPerSecondThreshold: 750000, queriesPerSecondThreshold: 55},
	influxDBTypes.DbInstanceTypeDbInflux16xlarge: influxDBInstanceSpecs{vCpu: 64, memory: 549755813888, networkBandwidth: 3125000000, seriesThreshold: 10000000, lineWritesPerSecondThreshold: 1000000, queriesPerSecondThreshold: 60},
	influxDBTypes.DbInstanceTypeDbInflux24xlarge: influxDBInstanceSpecs{vCpu: 96, memory: 824633720832, networkBandwidth: 5000000000, seriesThreshold: 10000000, lineWritesPerSecondThreshold: 1250000, queriesPerSecondThreshold: 75},
}

// getWorkspaceByName retrieves a Grafana workspace by its name.
// It lists all workspaces and finds the one matching the provided name.
// If the workspace is not immediately available, it will retry with a backoff strategy.
//
// Parameters:
//   - grafanaClient: The Grafana client used to make API calls
//   - workspaceName: The name of the workspace to find
//
// Returns:
//   - *grafanaTypes.WorkspaceSummary: The workspace summary if found
//   - error: An error if the workspace cannot be found or if there's an API error
func getWorkspaceByName(grafanaClient *grafana.Client, workspaceName string) (*grafanaTypes.WorkspaceSummary, error) {
	const SleepDuration = 5
	const MaxWaitIntervals = 200
	waiterInterval := 0

	resp, err := grafanaClient.ListWorkspaces(context.TODO(), &grafana.ListWorkspacesInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %v", err)
	}

	if resp == nil {
		for waiterInterval < MaxWaitIntervals {
			resp, err = grafanaClient.ListWorkspaces(context.TODO(), &grafana.ListWorkspacesInput{})
			if resp != nil {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list workspaces: %v", err)
			}
			waiterInterval++
			time.Sleep(SleepDuration * time.Second)
		}
	}

	if waiterInterval == MaxWaitIntervals {
		return nil, fmt.Errorf("failed to find workspace %s in workspaces", workspaceName)
	}

	for _, workspace := range resp.Workspaces {
		if *workspace.Name == workspaceName {
			return &workspace, nil
		}
	}

	return nil, fmt.Errorf("failed to find workspace %s in workspaces", workspaceName)
}

// getGrafanaHttpReq creates an HTTP request for Grafana API calls.
// It sets up the necessary headers including content type and authorization.
//
// Parameters:
//   - requestType: The HTTP method (GET, POST, etc.)
//   - urlWithEndpoint: The full URL with endpoint for the request
//   - payload: The request body
//   - serviceAccountTokenKey: The service account token for authorization
//
// Returns:
//   - *http.Request: The prepared HTTP request
//   - error: An error if the request creation fails
func getGrafanaHttpReq(requestType string, urlWithEndpoint string, payload io.Reader, serviceAccountTokenKey string) (*http.Request, error) {
	req, err := http.NewRequest(requestType, "https://"+urlWithEndpoint, payload)
	if err != nil {
		log.Printf("Failed to create Grafana HTTP request: %s", err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+serviceAccountTokenKey)
	return req, nil
}

// sendGrafanaHttpReq executes an HTTP request against the Grafana API.
//
// Parameters:
//   - httpClient: The HTTP client to use for the request
//   - req: The prepared HTTP request to send
//
// Returns:
//   - *http.Response: The HTTP response
//   - error: An error if the request execution fails
func sendGrafanaHttpReq(httpClient http.Client, req *http.Request) (*http.Response, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to execute Grafana HTTP request: %s", err)
		return nil, err
	}
	return resp, nil
}

// installInfinityPlugin installs the infinity plugin in the Grafana workspace.
//
// Parameters:
//   - workspaceUrl: The URL of the workspace
//   - serviceAccountTokenKey: The token to authenticate the request
//   - httpClient: The HTTP client to use for the request
//
// Returns:
//   - error: An error if the request execution fails
func installInfinityPlugin(workspaceUrl string, serviceAccountTokenKey string, httpClient http.Client) error {
	startTime := time.Now()
	const SleepDuration = 10
	const MaxWaitIntervals = 100
	var plugins []struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	installInfinityPluginReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/plugins/yesoreyeram-infinity-datasource/install",
		nil,
		serviceAccountTokenKey,
	)
	if err != nil {
		return err
	}
	installInfinityPluginResp, err := sendGrafanaHttpReq(httpClient, installInfinityPluginReq)
	if err != nil {
		log.Printf("Failed to install Inifnity plugin in Grafana workspace: %s", err)
		return err
	}

	if installInfinityPluginResp.StatusCode == http.StatusConflict {
		log.Printf("Infinity plugin is already installed in the Grafana workspace")
	} else if installInfinityPluginResp.StatusCode != http.StatusOK {
		log.Printf("Received status code %d when trying to install plugin: %s", installInfinityPluginResp.StatusCode, err)
		return fmt.Errorf("error: %d", installInfinityPluginResp.StatusCode)
	}
	log.Printf("Infinity data source installed")

	getGrafanaPluginsReq, err := getGrafanaHttpReq(
		"GET",
		workspaceUrl+"/api/plugins",
		nil,
		serviceAccountTokenKey,
	)
	if err != nil {
		return err
	}

	getGrafanaPluginsResp, err := sendGrafanaHttpReq(httpClient, getGrafanaPluginsReq)
	if err != nil {
		log.Printf("Failed to retrieve plugins for Grafana workspace: %s", err)
		return err
	}

	waiterInterval := 0
	for waiterInterval < MaxWaitIntervals {
		// Let Grafana catch up
		time.Sleep(SleepDuration * time.Second)
		body, err := io.ReadAll(getGrafanaPluginsResp.Body)
		if err != nil {
			log.Printf("Failed to read response body %s", err)
			return err
		}
		if err := json.Unmarshal(body, &plugins); err != nil {
			log.Printf("Failed to unmarshal JSON %s", err)
			return err
		}
		pluginInstalled := false
		for _, plugin := range plugins {
			if plugin.Name == "Infinity" {
				pluginInstalled = true
				break
			}
		}
		if pluginInstalled {
			break
		}
		getGrafanaPluginsResp, err = httpClient.Do(getGrafanaPluginsReq)
		if err != nil {
			log.Printf("failed to retrieve plugins for grafana workspace: %s", err)
			return err
		}

		waiterInterval++
	}

	if waiterInterval == MaxWaitIntervals {
		return fmt.Errorf("timeout reached for installing Infinity plugin in workspace")
	}

	// Grafana still requires additional time after plugin is listed installed
	time.Sleep(20 * time.Second)
	log.Printf("Installing infinity plugin took %v seconds to complete", time.Since(startTime).Seconds())
	return nil
}

// addDataSourceToWorkspace adds the passed in plugin as a datasource with the Grafana workspace.
//
// Parameters:
//   - datasourceConfig: Configuration options for the data source
//   - workspaceUrl: The URL of the workspace
//   - serviceAccountTokenKey: The token to authenticate the request
//   - httpClient: The HTTP client to use for the request
//
// Returns:
//   - error: An error if the request execution fails
func addDataSourceToWorkspace(datasourceConfig map[string]interface{}, workspaceUrl string, serviceAccountTokenKey string, httpClient http.Client) error {
	startTime := time.Now()
	jsonDataSourceConfig, err := json.Marshal(datasourceConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	configureGrafanaDatasourceReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/datasources",
		bytes.NewBuffer(jsonDataSourceConfig),
		serviceAccountTokenKey,
	)
	if err != nil {
		return err
	}
	configureGrafanaDatasourceResp, err := sendGrafanaHttpReq(httpClient, configureGrafanaDatasourceReq)
	if err != nil {
		log.Printf("Failed to configure %s: %s", datasourceConfig["name"], err)
		return err
	}

	if configureGrafanaDatasourceResp.StatusCode == http.StatusOK {
		log.Printf("%s successfully added to workspace", datasourceConfig["name"])
	} else if configureGrafanaDatasourceResp.StatusCode == http.StatusConflict {
		log.Printf("%s already exists in workspace", datasourceConfig["name"])
	} else {
		log.Printf("Failed to add %s to workspace with status code %d", datasourceConfig["name"], configureGrafanaDatasourceResp.StatusCode)
		return fmt.Errorf("failed to add %s: %d", datasourceConfig["name"], configureGrafanaDatasourceResp.StatusCode)
	}
	log.Printf("Adding datasource to workspace took %v seconds to complete", time.Since(startTime).Seconds())
	return nil
}

// uploadDashboard uploads a dashboard to a Grafana workspace.
// It first configures a CloudWatch data source if it doesn't exist,
// then creates and uploads the dashboard.
//
// Parameters:
//   - serviceAccountTokenKey: The service account token for authorization
//   - workspaceUrl: The URL of the Grafana workspace
//   - dashboardName: The name for the dashboard
//   - dbClusterInfo: Array of influxDBInstanceInfo
//   - influxDBVersion: The version of InfluxDB
//
// Returns:
//   - string: A success message if the dashboard was created successfully
//   - error: An error if any step fails
func uploadDashboard(serviceAccountTokenKey string, workspaceUrl string, dashboardName string, dbClusterInfo []influxDBInstanceInfo, dashboardDataGranularity string, influxDBVersion string) (string, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	err := installInfinityPlugin(workspaceUrl, serviceAccountTokenKey, *httpClient)
	if err != nil {
		return "", err
	}

	cloudWatchDataSourceConfig := map[string]interface{}{
		"name":   "Amazon CloudWatch DataSource",
		"type":   "cloudwatch",
		"access": "proxy",
		"jsonData": map[string]interface{}{
			"authType":      "default",
			"defaultRegion": os.Getenv("AWS_REGION"),
		},
	}

	err = addDataSourceToWorkspace(cloudWatchDataSourceConfig, workspaceUrl, serviceAccountTokenKey, *httpClient)
	if err != nil {
		return "", err
	}

	infinityDataSourceConfig := map[string]interface{}{
		"name":   "yesoreyeram-infinity-datasource",
		"type":   "yesoreyeram-infinity-datasource",
		"access": "proxy",
		"jsonData": map[string]interface{}{
			"global_queries": false,
		},
	}

	err = addDataSourceToWorkspace(infinityDataSourceConfig, workspaceUrl, serviceAccountTokenKey, *httpClient)
	if err != nil {
		return "", err
	}

	jsonDashboard, err := json.Marshal(generateDashboard(dashboardName, dbClusterInfo, dashboardDataGranularity, influxDBVersion))
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %v", err)
	}

	uploadGrafanaDashboardReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/dashboards/db",
		bytes.NewBuffer(jsonDashboard),
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	uploadGrafanaDashboardResp, err := sendGrafanaHttpReq(*httpClient, uploadGrafanaDashboardReq)
	if err != nil {
		log.Printf("Failed to upload Grafana dashboard: %s", err)
		return "", err
	}

	if uploadGrafanaDashboardResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error status code returned from uploading dashboard: %d", uploadGrafanaDashboardResp.StatusCode)
	}

	return "Dashboard created successfully", nil
}

// createGrafanaDashboard orchestrates the creation of a Grafana dashboard.
// It retrieves environment variables, sets up the Grafana client,
// creates or retrieves a service account, generates a token,
// and calls uploadDashboard to create the actual dashboard.
//
// Returns:
//   - string: A success message if the dashboard was created successfully
//   - error: An error if any step fails
func createGrafanaDashboard() (string, error) {
	const SleepDuration = 5
	const MaxWaitIntervals = 200

	workspaceName := os.Getenv("GrafanaWorkspaceName")
	if workspaceName == "" {
		return "", fmt.Errorf("Failed to get GrafanaWorkspaceName environment variable")
	}
	dashboardName := os.Getenv("DashboardName")
	if dashboardName == "" {
		return "", fmt.Errorf("Failed to get DashboardName environment variable")
	}
	dbClusterInfo := os.Getenv("DbClusterInfo")
	if dbClusterInfo == "" {
		return "", fmt.Errorf("Failed to get DbClusterInfo environment variable")
	}
	clusterInfo := []influxDBInstanceInfo{}
	for _, instanceInfo := range strings.Split(dbClusterInfo, ",") {
		dbInfo := strings.Split(instanceInfo, ":")
		if len(dbInfo) != 3 {
			return "", fmt.Errorf("DbInstanceInfo variable not in the correct format db-name:db-size:db-storage-type: %s", dbInfo)
		}
		clusterInfo = append(clusterInfo,
			influxDBInstanceInfo{
				instanceName:        dbInfo[InstanceNameIdx],
				instanceSize:        influxDBTypes.DbInstanceType(dbInfo[InstanceSizeIdx]),
				instanceStorageType: influxDBTypes.DbStorageType(dbInfo[InstanceStorageTypeIdx]),
			},
		)
	}

	dashboardDataGranularity := os.Getenv("dashboardDataGranularity")
	if dashboardDataGranularity == "" {
		return "", fmt.Errorf("Failed to get dashboardDataGranularity environment variable")
	}
	influxDBVersion := os.Getenv("influxDBVersion")
	if influxDBVersion == "" {
		return "", fmt.Errorf("Failed to get influxDBVersion environment variable")
	}

	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("error loading default AWS config: %s", err)
		return "", err
	}

	grafanaServiceAccountTokenName := "ADMIN"
	var grafanaServiceAccountTokenSecondsToLive int32 = 86400 //24 hours
	grafanaClient := grafana.NewFromConfig(awsConfig)

	serviceAccountTokenKey := ""
	grafanaWorkspace, err := getWorkspaceByName(grafanaClient, workspaceName)
	if err != nil {
		log.Printf("Failed to get workspace: %s", err)
		return "", err
	}

	waiterInterval := 0

	if grafanaWorkspace.Status != grafanaTypes.WorkspaceStatusActive {
		for waiterInterval < MaxWaitIntervals {
			grafanaWorkspace, err = getWorkspaceByName(grafanaClient, workspaceName)
			if err != nil {
				log.Printf("Failed to get workspace: %s", err)
				return "", err
			}
			if grafanaWorkspace.Status == grafanaTypes.WorkspaceStatusActive {
				break
			}
			waiterInterval++
			time.Sleep(SleepDuration * time.Second)
		}
	}

	if waiterInterval == MaxWaitIntervals {
		return "", fmt.Errorf("Failed to create service account due to workspace creation timeout")
	}

	var maxWorkspaceServiceAccountsResult int32 = 200
	workspaceServiceAccounts, err := grafanaClient.ListWorkspaceServiceAccounts(
		context.TODO(),
		&grafana.ListWorkspaceServiceAccountsInput{
			WorkspaceId: grafanaWorkspace.Id,
			MaxResults:  &maxWorkspaceServiceAccountsResult,
		},
	)
	if err != nil {
		log.Printf("Error listing workspace service accounts: %s", err)
		return "", err
	}

	serviceAccountID := ""
	for _, serviceAccount := range workspaceServiceAccounts.ServiceAccounts {
		if *serviceAccount.Name == workspaceName {
			serviceAccountID = *serviceAccount.Id
			break
		}
	}

	if serviceAccountID == "" {
		serviceAccountOutput, err := grafanaClient.CreateWorkspaceServiceAccount(
			context.TODO(),
			&grafana.CreateWorkspaceServiceAccountInput{
				GrafanaRole: grafanaTypes.RoleAdmin,
				Name:        &workspaceName,
				WorkspaceId: grafanaWorkspace.Id,
			},
		)
		if err != nil {
			log.Printf("Error listing workspace service accounts: %s", err)
			return "", err
		}
		serviceAccountID = *serviceAccountOutput.Id
	} else {
		serviceAccountTokens, err := grafanaClient.ListWorkspaceServiceAccountTokens(context.TODO(), &grafana.ListWorkspaceServiceAccountTokensInput{
			ServiceAccountId: &serviceAccountID,
			WorkspaceId:      grafanaWorkspace.Id,
			MaxResults:       &maxWorkspaceServiceAccountsResult,
		})
		if err != nil {
			log.Printf("Error listing workspace service account tokens: %s", err)
			return "", err
		}
		for _, serviceAccountToken := range serviceAccountTokens.ServiceAccountTokens {
			if *serviceAccountToken.Name == "ADMIN" {
				log.Printf("Existing service account token exists which needs to be deleted and re-created")
				grafanaClient.DeleteWorkspaceServiceAccountToken(context.TODO(), &grafana.DeleteWorkspaceServiceAccountTokenInput{
					ServiceAccountId: &serviceAccountID,
					TokenId:          serviceAccountToken.Id,
					WorkspaceId:      grafanaWorkspace.Id,
				})
				break
			}
		}
	}

	serviceAccountTokenOutput, err := grafanaClient.CreateWorkspaceServiceAccountToken(context.TODO(), &grafana.CreateWorkspaceServiceAccountTokenInput{
		Name:             &grafanaServiceAccountTokenName,
		SecondsToLive:    &grafanaServiceAccountTokenSecondsToLive,
		ServiceAccountId: &serviceAccountID,
		WorkspaceId:      grafanaWorkspace.Id,
	})
	if err != nil {
		log.Printf("Error getting Grafana service account token: %s", err)
		return "", err
	}
	if serviceAccountTokenOutput.ServiceAccountToken.Key == nil {
		log.Printf("Error getting Grafana service account token: %s", err)
		return "", fmt.Errorf("Failed to get token key")
	}
	serviceAccountTokenKey = *serviceAccountTokenOutput.ServiceAccountToken.Key

	// The endpoint may take time to populate from the workspace output
	waiterInterval = 0
	for waiterInterval < MaxWaitIntervals {
		grafanaWorkspace, err = getWorkspaceByName(grafanaClient, workspaceName)
		if err != nil {
			log.Printf("Failed to get workspace: %s", err)
			return "", err
		}
		if grafanaWorkspace.Endpoint != nil {
			break
		}
		waiterInterval++
		time.Sleep(SleepDuration * time.Second)
	}

	ret, err := uploadDashboard(serviceAccountTokenKey, *grafanaWorkspace.Endpoint, dashboardName, clusterInfo, dashboardDataGranularity, influxDBVersion)
	if err != nil {
		return "", err
	}

	return ret, nil
}

// Entry point for the Lambda function. Calls createGrafanaDashboard and returns API Gateway response.
//
// Parameters:
//   - ctx: The Lambda context
//   - event: The Lambda event data
//
// Returns:
//   - events.APIGatewayProxyResponse: The API Gateway response
//   - error: An error if the function execution fails
func lambdaHandler(ctx context.Context, event map[string]interface{}) (events.APIGatewayProxyResponse, error) {
	resp, err := createGrafanaDashboard()
	if err != nil {
		log.Printf("Error creating Grafana dashboard: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error creating Grafana dashboard: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       resp,
	}, nil
}

// debugDashboard generates and pretty prints the dashboard JSON for local debugging.
//
// Returns:
//   - error: on failure to create JSON dashboard
func debugDashboard() error {
	dashboardName := "debugDashboard"
	dbClusterInfo := "dbInstance1:db.influx.16xlarge:InfluxIOIncludedT3,dbInstance2:db.influx.12large:InfluxIOIncludedT2"
	influxDBVersion := "3"

	clusterInfo := []influxDBInstanceInfo{}
	for _, instanceInfo := range strings.Split(dbClusterInfo, ",") {
		dbInfo := strings.Split(instanceInfo, ":")
		if len(dbInfo) != 3 {
			return fmt.Errorf("DbInstanceInfo variable not in the correct format db-name:db-size:db-storage-type: %s", dbInfo)
		}
		clusterInfo = append(clusterInfo,
			influxDBInstanceInfo{
				instanceName:        dbInfo[InstanceNameIdx],
				instanceSize:        influxDBTypes.DbInstanceType(dbInfo[InstanceSizeIdx]),
				instanceStorageType: influxDBTypes.DbStorageType(dbInfo[InstanceStorageTypeIdx]),
			},
		)
	}

	dashboardDataGranularity := "10s"
	dashboard := generateDashboard(dashboardName, clusterInfo, dashboardDataGranularity, influxDBVersion)
	prettyJSON, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}
	fmt.Println(string(prettyJSON))
	return nil
}

// Start the Lambda handler.
func main() {
	debugMode := flag.Bool("debug", false, "Run in debug mode to pretty print the dashboard JSON")
	flag.Parse()

	if *debugMode {
		if err := debugDashboard(); err != nil {
			log.Fatalf("Error in debug mode: %v", err)
		}
	} else {
		lambda.Start(lambdaHandler)
	}
}

// panelField represents the configuration for a dashboard panel.
// It contains information about the panel's position, title, type,
// statistic type, and the metrics to display.
type panelField struct {
	gridPosition  map[string]interface{}
	title         string
	panelType     string
	statisticType string
	unit          string
	metricNames   []string
	mathQuery     string
}

// generatePanelOptions creates the options configuration for a dashboard panel
// based on the panel type.
//
// Parameters:
//   - panelType: The type of panel (stat, bargauge, gauge, etc.)
//
// Returns:
//   - map[string]interface{}: The panel options configuration
func generatePanelOptions(panelType string) map[string]interface{} {
	switch panelType {
	case "stat":
		return map[string]interface{}{
			"colorMode":   "value",
			"graphMode":   "area",
			"justifyMode": "auto",
			"orientation": "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showPercentChange": false,
			"textMode":          "auto",
			"wideLayout":        true,
		}
	case "bargauge":
		return map[string]interface{}{
			"minVizHeight":  16,
			"minVizWidth":   8,
			"maxVizHeight":  300,
			"orientation":   "auto",
			"displayMode":   "gradient",
			"valueMode":     "color",
			"namePlacement": "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showUnfilled":         true,
			"showThresholdLabels":  false,
			"showThresholdMarkers": true,
			"sizing":               "auto",
		}
	case "gauge":
		return map[string]interface{}{
			"minVizHeight": 75,
			"minVizWidth":  75,
			"orientation":  "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showThresholdLabels":  false,
			"showThresholdMarkers": true,
			"sizing":               "auto",
		}
	default:
		return map[string]interface{}{}
	}
}

// generatePanelFieldConfig creates the field configuration for a dashboard panel
// based on the panel type and title.
//
// Parameters:
//   - panelType: The type of panel (stat, bargauge, gauge, etc.)
//   - panelTitle: The title of the panel
//   - panelUnit: The unit used for the panel
//
// Returns:
//   - map[string]interface{}: The panel field configuration
func generatePanelFieldConfig(panelType string, panelTitle string, panelUnit string) map[string]interface{} {
	switch panelType {
	case "stat":
		if panelUnit == "none" {
			return map[string]interface{}{
				"defaults": map[string]interface{}{
					"mappings": []string{},
					"thresholds": map[string]interface{}{
						"mode": "absolute",
						"steps": []interface{}{
							map[string]interface{}{
								"color": "yellow",
								"value": nil,
							},
							map[string]interface{}{
								"color": "green",
								"value": 1,
							},
						},
					},
					"noValue": "0",
				},
				"overrides": []string{},
			}
		} else {
			return map[string]interface{}{
				"defaults": map[string]interface{}{
					"mappings": []string{},
					"thresholds": map[string]interface{}{
						"mode": "absolute",
						"steps": []interface{}{
							map[string]interface{}{
								"color": "green",
								"value": nil,
							},
						},
					},
					"noValue": 0,
					"unit":    "bytes",
				},
				"overrides": []string{},
			}
		}
	case "bargauge":
		return map[string]interface{}{"defaults": map[string]interface{}{
			"mappings": []string{},
			"thresholds": map[string]interface{}{
				"mode": "absolute",
				"steps": []interface{}{
					map[string]interface{}{
						"color": "yellow",
						"value": nil,
					},
					map[string]interface{}{
						"color": "green",
						"value": 1,
					},
				},
			},
			"color": map[string]interface{}{
				"mode": "thresholds",
			},
		},
			"overrides": []interface{}{
				map[string]interface{}{
					"matcher": map[string]interface{}{
						"id":      "byName",
						"options": "sum",
					},
					"properties": []interface{}{
						map[string]interface{}{
							"id": "thresholds",
							"value": map[string]interface{}{
								"mode": "absolute",
								"steps": []interface{}{
									map[string]interface{}{
										"color": "green",
										"value": nil,
									},
									map[string]interface{}{
										"color": "yellow",
										"value": 1,
									},
									map[string]interface{}{
										"color": "red",
										"value": 50,
									},
								},
							},
						},
					},
				},
			},
		}
	case "gauge":
		return map[string]interface{}{"defaults": map[string]interface{}{
			"mappings": []string{},
			"thresholds": map[string]interface{}{
				"mode": "percentage",
				"steps": []interface{}{
					map[string]interface{}{
						"color": "green",
						"value": nil,
					},
					map[string]interface{}{
						"color": "yellow",
						"value": 60,
					},
					map[string]interface{}{
						"color": "red",
						"value": 80,
					},
				},
			},
			"color": map[string]interface{}{
				"mode": "thresholds",
			},
			"unit": "percent",
			"min":  0,
			"max":  100,
		},
			"overrides": []interface{}{},
		}
	default:
		return map[string]interface{}{}
	}
}

// generatePanels creates all the panels for the dashboard.
// It defines the panel fields and generates the configuration for each panel.
//
// Parameters:
//   - dashboardDataGranularity: Granularity of data to use in dashboard 60s by default and 5s for high granularity
//   - influxDBVersion: The version of InfluxDB
//
// Returns:
//   - []interface{}: An array of panel configurations
func generatePanels(dashboardDataGranularity string, influxDBVersion string) []interface{} {

	var panelFields []panelField

	if influxDBVersion == "2" {
		panelFields = []panelField{
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 0}, "Memory utilization", "gauge", "Average", "percent", []string{"MemoryUtilization"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 8}, "CPU utilization", "gauge", "Average", "percent", []string{"CPUUtilization"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 8}, "Disk utilization", "gauge", "Average", "percent", []string{"DiskUtilization"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 16}, "Total Go system memory usage", "gauge", "Maximum", "bytes", []string{"go_memstats_sys_bytes_gauge"}, "($A / ${instanceMemory}) * 100"},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 24}, "Memory cache usage", "stat", "Maximum", "bytes", []string{"go_memstats_mcache_inuse_bytes_gauge"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 24}, "BoltDb writes", "stat", "Maximum", "none", []string{"boltdb_writes_total_counter"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 32}, "System bytes in-use", "stat", "Maximum", "bytes", []string{"go_memstats_alloc_bytes_gauge"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 32}, "HTTP write requests count", "stat", "Maximum", "none", []string{"http_write_request_count_counter"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 40}, "HTTP query requests count", "stat", "Maximum", "none", []string{"http_query_request_count_counter"}, ""},
		}
	} else {
		panelFields = []panelField{
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 0}, "Memory utilization", "gauge", "Average", "percent", []string{"MemoryUtilization"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 8}, "CPU utilization", "gauge", "Average", "percent", []string{"CPUUtilization"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 24}, "Parquet cache size", "stat", "Maximum", "bytes", []string{"influxdb3_parquet_cache_size_bytes_gauge"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 24}, "Total bytes written", "stat", "Maximum", "bytes", []string{"influxdb3_write_bytes_total_counter"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 32}, "Database bytes in-use", "stat", "Maximum", "bytes", []string{"jemalloc_memstats_bytes_gauge"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 32}, "HTTP requests count", "stat", "Maximum", "none", []string{"http_requests_total_counter"}, ""},
			{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 40}, "Grpc requests count", "stat", "Maximum", "none", []string{"grpc_requests_total_counter"}, ""},
		}
	}

	var panelConfig []interface{}
	for _, panel := range panelFields {

		var panelTargets []interface{}
		var refId byte = 'A'
		for _, metricName := range panel.metricNames {
			hidePanel := false
			if panel.mathQuery != "" {
				hidePanel = true
			}

			panelQuery := map[string]interface{}{
				"datasource": "Amazon CloudWatch DataSource",
				"region":     "default",
				"logGroups":  []interface{}{},
				"queryMode":  "Metrics",
				"namespace":  "AWS/Timestream/InfluxDB",
				"metricName": metricName,
				"expression": "",
				"dimensions": map[string]interface{}{
					"DbInstanceName": "$instanceName",
				},
				"statistic":        panel.statisticType,
				"period":           dashboardDataGranularity,
				"metricQueryType":  0,
				"metricEditorMode": 0,
				"sqlExpression":    "",
				"matchExact":       false,
				"refId":            string(refId),
				"hide":             hidePanel,
				"label":            "",
			}

			panelTargets = append(
				panelTargets,
				panelQuery,
			)
			// Optional additional math query for using instance specifications
			if panel.mathQuery != "" {
				refId += byte(1)
				panelTargets = append(
					panelTargets,
					map[string]interface{}{
						"datasource": map[string]interface{}{
							"name": "Expression",
							"type": "__expr__",
							"uid":  "__expr__",
						},
						"expression": panel.mathQuery,
						"hide":       false,
						"refId":      string(refId),
						"type":       "math",
					},
				)
			}
			// Up one letter in alphabet
			refId += byte(1)
		}

		panelConfig = append(
			panelConfig,
			map[string]interface{}{
				"gridPos":     panel.gridPosition,
				"targets":     panelTargets,
				"title":       panel.title,
				"type":        panel.panelType,
				"options":     generatePanelOptions(panel.panelType),
				"fieldConfig": generatePanelFieldConfig(panel.panelType, panel.title, panel.unit),
			},
		)
	}

	return panelConfig
}

// getInfinityVariableConfig generates the cluster variable configuration for a specific instance specifications.
//
// Parameters:
//   - instanceSpec: The instance specification for the cluster
//   - variableName: The variable name to use with Infinity data source and the dashboard
//
// Returns:
//   - map[string]interface{}: The Infinity data source config
func getInfinityVariableConfig(instanceSpec map[string]interface{}, variableName string) map[string]interface{} {

	infinityQuery, _ := json.Marshal(instanceSpec)
	infinityVariable := map[string]interface{}{}
	infinityVariable["datasource"] = map[string]interface{}{
		"type": "yesoreyeram-infinity-datasource",
		"uid":  "yesoreyeram-infinity-datasource",
	}
	infinityVariable["definition"] = "yesoreyeram-infinity-datasource- (infinity) json"
	infinityVariable["description"] = ""
	infinityVariable["hide"] = 2
	infinityVariable["includeAll"] = false
	infinityVariable["multi"] = false
	infinityVariable["name"] = variableName
	infinityVariable["options"] = []interface{}{}
	infinityVariable["query"] = map[string]interface{}{
		"infinityQuery": map[string]interface{}{
			"columns":       []interface{}{},
			"data":          string(infinityQuery),
			"filters":       []interface{}{},
			"format":        "table",
			"parser":        "backend",
			"refId":         "variable",
			"root_selector": "$instanceName.0",
			"source":        "inline",
			"type":          "json",
			"url":           "",
			"url_options": map[string]interface{}{
				"data":   "",
				"method": "GET",
			},
		},
		"query":     "",
		"queryType": "infinity",
	}
	infinityVariable["refresh"] = 1
	infinityVariable["regex"] = ""
	infinityVariable["skipUrlSync"] = false
	infinityVariable["sort"] = 0
	infinityVariable["type"] = "query"

	return infinityVariable
}

// generateDashboard creates the complete dashboard configuration.
// It sets up the dashboard with panels, templates for instance selection,
// and time range settings.
//
// Parameters:
//   - dashboardName: The name of the dashboard
//   - dbClusterInfo: An array of influxDBInstanceInfo
//   - dashboardDataGranularity: Granularity of data to use in dashboard 60s by default and 5s for high granularity
//   - influxDBVersion: The version of InfluxDB
//
// Returns:
//   - map[string]interface{}: The complete dashboard configuration
func generateDashboard(dashboardName string, dbClusterInfo []influxDBInstanceInfo, dashboardDataGranularity string, influxDBVersion string) map[string]interface{} {

	currentTemplateOptions := map[string]interface{}{}
	templateOptions := []interface{}{}
	infinityVariables := []interface{}{}
	firstOption := true
	queryString := ""
	infinityClusterCpu := map[string]interface{}{}
	infinityClusterMemory := map[string]interface{}{}
	infinityClusterNetwork := map[string]interface{}{}
	infinityClusterSeries := map[string]interface{}{}
	infinityClusterLineWrites := map[string]interface{}{}
	infinityClusterQueries := map[string]interface{}{}

	for _, dbInstanceInfo := range dbClusterInfo {
		if queryString != "" {
			queryString += ","
		}
		queryString += dbInstanceInfo.instanceName

		templateOptions = append(
			templateOptions, map[string]interface{}{
				"selected": firstOption,
				"text":     dbInstanceInfo.instanceName,
				"value":    dbInstanceInfo.instanceName,
			},
		)

		if firstOption {
			currentTemplateOptions["current"] = map[string]interface{}{
				"selected": false,
				"text":     dbInstanceInfo.instanceName,
				"value":    dbInstanceInfo.instanceName,
			}
			firstOption = false
		}

		infinityClusterCpu[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].vCpu}
		infinityClusterMemory[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].memory}
		infinityClusterNetwork[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].networkBandwidth}
		infinityClusterSeries[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].seriesThreshold}
		infinityClusterLineWrites[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].lineWritesPerSecondThreshold}
		infinityClusterQueries[dbInstanceInfo.instanceName] = []int{influxDBInstanceTypes[dbInstanceInfo.instanceSize].queriesPerSecondThreshold}
	}

	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterCpu, "instanceCpu"))
	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterMemory, "instanceMemory"))
	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterNetwork, "instanceNetwork"))
	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterSeries, "instanceSeries"))
	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterLineWrites, "instanceLineWrites"))
	infinityVariables = append(infinityVariables, getInfinityVariableConfig(infinityClusterQueries, "instanceQueries"))

	dashboardConfig := map[string]interface{}{
		"overwrite": true,
		"folder":    0,
		"dashboard": map[string]interface{}{
			"panels": generatePanels(dashboardDataGranularity, influxDBVersion),
			"title":  dashboardName,
			"templating": map[string]interface{}{
				"list": append([]interface{}{
					map[string]interface{}{
						"current":     currentTemplateOptions["current"],
						"hide":        0,
						"includeAll":  false,
						"label":       "",
						"multi":       false,
						"name":        "instanceName", // Variable name used in Grafana
						"options":     templateOptions,
						"query":       queryString,
						"queryValue":  "",
						"skipUrlSync": false,
						"type":        "custom",
					},
				}, infinityVariables...),
			},
		},
		"time": map[string]interface{}{
			"from": "now-15m",
			"to":   "now",
		},
	}

	return dashboardConfig
}
