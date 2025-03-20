package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanaTypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
)

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

func sendGrafanaHttpReq(httpClient http.Client, req *http.Request) (*http.Response, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to execute Grafana HTTP request: %s", err)
		return nil, err
	}
	return resp, nil
}

func uploadDashboard(serviceAccountTokenKey string, workspaceUrl string, datasourceName string, dashboardName string, databaseName string) (string, error) {
	const SleepDuration = 10
	const MaxWaitIntervals = 100

	var plugins []struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	installTimestreamPluginReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/plugins/grafana-timestream-datasource/install",
		nil,
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	installTimestreamPluginResp, err := sendGrafanaHttpReq(*httpClient, installTimestreamPluginReq)
	if err != nil {
		log.Printf("Failed to install Timestream plugin in Grafana workspace: %s", err)
		return "", err
	}

	if installTimestreamPluginResp.StatusCode == http.StatusConflict {
		log.Printf("Timestream plugin is already installed in the Grafana workspace")
	} else if installTimestreamPluginResp.StatusCode != http.StatusOK {
		log.Printf("Received status code %d when trying to install plugin: %s", installTimestreamPluginResp.StatusCode, err)
		return "", fmt.Errorf("error: %d", installTimestreamPluginResp.StatusCode)
	}
	log.Printf("Timestream data source installed")

	getGrafanaPluginsReq, err := getGrafanaHttpReq(
		"GET",
		workspaceUrl+"/api/plugins",
		nil,
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}

	getGrafanaPluginsResp, err := sendGrafanaHttpReq(*httpClient, getGrafanaPluginsReq)
	if err != nil {
		log.Printf("Failed to retrieve plugins for Grafana workspace: %s", err)
		return "", err
	}

	waiterInterval := 0
	for waiterInterval < MaxWaitIntervals {
		// Let Grafana catch up
		time.Sleep(SleepDuration * time.Second)
		body, err := io.ReadAll(getGrafanaPluginsResp.Body)
		if err != nil {
			log.Printf("Failed to read response body %s", err)
			return "", err
		}
		if err := json.Unmarshal(body, &plugins); err != nil {
			log.Printf("Failed to unmarshal JSON %s", err)
			return "", err
		}
		pluginInstalled := false
		for _, plugin := range plugins {
			if plugin.Name == "Amazon Timestream" {
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
			return "", err
		}

		waiterInterval++
	}

	if waiterInterval == MaxWaitIntervals {
		return "", fmt.Errorf("timeout reached for installing Timestream plugin in workspace")
	}

	// Grafana still requires additional time after plugin is listed installed
	time.Sleep(20 * time.Second)

	enablePluginConfig := map[string]interface{}{
		"enabled": true,
		"pinned":  true,
	}

	jsonPluginConfig, err := json.Marshal(enablePluginConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %v", err)
	}

	enableGrafanaPluginReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/plugins/grafana-timestream-datasource/settings",
		bytes.NewBuffer(jsonPluginConfig),
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	enableGrafanaPluginResp, err := sendGrafanaHttpReq(*httpClient, enableGrafanaPluginReq)
	if err != nil {
		log.Printf("Failed to enable Grafana plugin: %s", err)
		return "", err
	}

	if enableGrafanaPluginResp.StatusCode == http.StatusOK {
		log.Printf("Timestream plugin successfully enabled")
	} else if enableGrafanaPluginResp.StatusCode == http.StatusConflict {
		log.Printf("Timestream plugin already enabled")
	} else {
		log.Printf("Failed to enable with status code %d", enableGrafanaPluginResp.StatusCode)
		return "", fmt.Errorf("failed to enable Timestream plugin: %d", enableGrafanaPluginResp.StatusCode)
	}

	log.Printf("Timestream plugin installed")
	dataSourceConfig := map[string]interface{}{
		"name":   datasourceName,
		"type":   "grafana-timestream-datasource",
		"access": "proxy",
		"jsonData": map[string]interface{}{
			"defaultRegion":      os.Getenv("AWS_REGION"),
			"database":           "",
			"table":              "",
			"authenticationType": "AWS_IAM",
		},
	}

	jsonDataSourceConfig, err := json.Marshal(dataSourceConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %v", err)
	}

	configureGrafanaDatasourceReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/datasources",
		bytes.NewBuffer(jsonDataSourceConfig),
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	configureGrafanaDatasourceResp, err := sendGrafanaHttpReq(*httpClient, configureGrafanaDatasourceReq)
	if err != nil {
		log.Printf("Failed to configure Timestream data source: %s", err)
		return "", err
	}

	if configureGrafanaDatasourceResp.StatusCode == http.StatusOK {
		log.Printf("Timestream data source successfully added to workspace")
	} else if configureGrafanaDatasourceResp.StatusCode == http.StatusConflict {
		log.Printf("Timestream data source already exists in workspace")
	} else {
		log.Printf("Failed to add Timestream data source to workspace with status code %d", configureGrafanaDatasourceResp.StatusCode)
		return "", fmt.Errorf("failed to add Timestream data source: %d", configureGrafanaDatasourceResp.StatusCode)
	}

	jsonDashboard, err := json.Marshal(generateDashboard(datasourceName, dashboardName, databaseName))
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

func createGrafanaDashboard() (string, error) {
	const SleepDuration = 5
	const MaxWaitIntervals = 200

	workspaceName := os.Getenv("GrafanaWorkspaceName")
	if workspaceName == "" {
		return "", fmt.Errorf("Failed to get GrafanaWorkspaceName environment variable")
	}
	datasourceName := os.Getenv("TimestreamDatasourceName")
	if datasourceName == "" {
		return "", fmt.Errorf("Failed to get TimestreamDatasourceName environment variable")
	}
	dashboardName := os.Getenv("DashboardName")
	if dashboardName == "" {
		return "", fmt.Errorf("Failed to get DashboardName environment variable")
	}
	databaseName := os.Getenv("DatabaseName")
	if databaseName == "" {
		return "", fmt.Errorf("Failed to get DatabaseName environment variable")
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
					TokenId:          *&serviceAccountToken.Id,
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

	ret, err := uploadDashboard(serviceAccountTokenKey, *grafanaWorkspace.Endpoint, datasourceName, dashboardName, databaseName)
	if err != nil {
		return "", err
	}

	return ret, nil
}

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

func main() {
	lambda.Start(lambdaHandler)
}

type panelField struct {
	gridPosition map[string]interface{}
	title        string
	panelType    string
	refId        string
	query        string
}

func generateBucketedGaugePanelQuery(instanceName string, databaseName string, tableName string) string {
	return fmt.Sprintf(`SELECT CONCAT('ID: ', %s), CONCAT('bucket: ', bucket), gauge FROM "%s"."%s"
  WHERE time = (
    SELECT MAX(time)
      FROM "%s"."%s" as subquery
  	  WHERE subquery.%s = "%s".%s
  	AND subquery.bucket = "%s".bucket
)
ORDER BY %s, bucket LIMIT 25`, instanceName, databaseName, tableName, databaseName, tableName, instanceName, tableName, instanceName, tableName, instanceName)
}

func generateGaugePanelQuery(instanceName string, databaseName string, tableName string) string {
	return fmt.Sprintf(`SELECT CONCAT('ID: ', %s), gauge FROM "%s"."%s"
  WHERE time = (
    SELECT MAX(time)
      FROM "%s"."%s" as subquery
  	  WHERE subquery.%s = "%s".%s
)
ORDER BY %s LIMIT 25`, instanceName, databaseName, tableName, databaseName, tableName, instanceName, tableName, instanceName, instanceName)
}

func generateCounterStatPanelQuery(instanceName string, databaseName string, tableName string) string {
	return fmt.Sprintf("SELECT CONCAT('ID: ', %s), MAX(counter) FROM \"%s\".\"%s\" GROUP BY %s ORDER BY %s DESC LIMIT 25", instanceName, databaseName, tableName, instanceName, instanceName)
}

func generateEndpointCounterStatPanelQuery(instanceName string, databaseName string, tableName string, endpoint string) string {
	return fmt.Sprintf("SELECT CONCAT('ID: ', %s), MAX(counter) FROM \"%s\".\"%s\" WHERE endpoint LIKE '%s' GROUP BY %s ORDER BY %s DESC LIMIT 25", instanceName, databaseName, tableName, endpoint, instanceName, instanceName)
}

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
				"values": true,
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
	default:
		return map[string]interface{}{}
	}
}

func generatePanelFieldConfig(panelType string, panelTitle string) map[string]interface{} {
	switch panelType {
	case "stat":
		if panelTitle == "HTTP Write Requests Count" || panelTitle == "HTTP Query Requests Count" || panelTitle == "Bucket Cardinality" || panelTitle == "BoltDb Writes" {
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
	default:
		return map[string]interface{}{}
	}
}

func generatePanels(datasourceName string, databaseName string) []interface{} {

	panelFields := []panelField{
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 0}, "Query Execution Duration in Seconds", "bargauge", "M", fmt.Sprintf("SELECT * FROM \"%s\".\"qc_executing_duration_seconds\" ORDER BY time desc", databaseName)},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 0}, "Total Available Memory from System", "stat", "L", generateGaugePanelQuery("influxDBInstance", databaseName, "go_memstats_sys_bytes")},
		{map[string]interface{}{"h": 6, "w": 7, "x": 0, "y": 22}, "Bucket Cardinality", "stat", "A", generateBucketedGaugePanelQuery("influxDBInstance", databaseName, "storage_bucket_series_num")},
		{map[string]interface{}{"h": 6, "w": 6, "x": 7, "y": 22}, "Memory Cache Usage", "stat", "B", generateGaugePanelQuery("influxDBInstance", databaseName, "go_memstats_mcache_inuse_bytes")},
		{map[string]interface{}{"h": 6, "w": 6, "x": 13, "y": 22}, "BoltDb Writes", "stat", "C", generateCounterStatPanelQuery("influxDBInstance", databaseName, "boltdb_writes_total")},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 30}, "Allocated Memory", "stat", "G", generateGaugePanelQuery("influxDBInstance", databaseName, "go_memstats_alloc_bytes")},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 30}, "HTTP Write Requests Count", "stat", "I", generateEndpointCounterStatPanelQuery("influxDBInstance", databaseName, "http_write_request_count", "%write%")},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 30}, "HTTP Query Requests Count", "stat", "N", generateEndpointCounterStatPanelQuery("influxDBInstance", databaseName, "http_query_request_count", "%query%")},
	}

	var panelConfig []interface{}
	for _, panel := range panelFields {
		panelConfig = append(
			panelConfig,
			map[string]interface{}{
				"gridPos": panel.gridPosition,
				"targets": []interface{}{
					map[string]interface{}{
						"datasource": datasourceName,
						"format":     0,
						"rawQuery":   panel.query,
						"refId":      panel.refId,
					},
				},
				"title":       panel.title,
				"type":        panel.panelType,
				"options":     generatePanelOptions(panel.panelType),
				"fieldConfig": generatePanelFieldConfig(panel.panelType, panel.title),
			},
		)
	}

	return panelConfig
}

func generateDashboard(datasourceName string, dashboardName string, databaseName string) map[string]interface{} {
	dashboardConfig := map[string]interface{}{
		"overwrite": true,
		"folder":    0,
		"dashboard": map[string]interface{}{
			"panels": generatePanels(datasourceName, databaseName),
			"title":  dashboardName,
		},
		"time": map[string]interface{}{
			"from": "now-15m",
			"to":   "now",
		},
	}

	return dashboardConfig
}
