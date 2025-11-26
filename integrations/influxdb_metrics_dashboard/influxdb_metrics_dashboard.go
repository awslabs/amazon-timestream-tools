package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsgrafana"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/customresources"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/jsii-runtime-go"
)

// getBaseTelegrafConfig generates the base Telegraf configuration with agent settings
// and processors for metric conversion and filtering. The Starlark processor filters
// out any tags that are not for the db instance name, bucket, or org. This reduces
// cardinality of the dataset while providing basic statistics for the Grafana dashboard.
// As well only counter and gauge metric types are supported as CloudWatch does not natively
// support histogram types and for high cardinality datasets cause large amounts of metrics
// to be ingested to CloudWatch.
//
// Parameters:
//   - configVars: A map containing template variables for the configuration,
//     including the interval for metric collection and flushing
//
// Returns:
//   - A string containing the rendered Telegraf base configuration
func getBaseTelegrafConfig(configVars map[string]string) string {
	telegrafBaseConf := `[agent]
  interval = "{{.Interval}}"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "{{.Interval}}"
  flush_jitter = "0s"
  precision = ""
  hostname = ""
  omit_hostname = false

  [[processors.converter]]
    [processors.converter.fields]
      float = ["*"]

[[processors.starlark]]
  source = '\'''\'''\''
def apply(metric):

  supported_tags = ["DbInstanceName", "bucket", "org"]
  if "counter" in metric.fields or "gauge" in metric.fields:
    for tagKey, tagVal in metric.tags.items():
      if tagKey not in supported_tags:
        metric.tags.pop(tagKey)
    return metric

  return None
'\'''\'''\''


`

	telegrafTmpl, err := template.New("telegrafConfigTemplate").Parse(telegrafBaseConf)
	if err != nil {
		log.Printf("Failed creating new template for Telegraf base config: %s", err.Error())
		os.Exit(1)
	}

	var templateBuf bytes.Buffer
	if err := telegrafTmpl.Execute(&templateBuf, configVars); err != nil {
		log.Printf("Failed to execute template for Telegraf base config: %s", err.Error())
		os.Exit(1)
	}
	return templateBuf.String()
}

// getTelegrafPluginsConfig generates the Telegraf plugin configuration for CloudWatch output
// and Prometheus input (metrics endpoint format) to collect metrics from InfluxDB instances.
//
// Parameters:
//   - configVars: A map containing template variables for the configuration,
//     including region, instance name, endpoint, and high resolution metrics flag
//   - influxDBVersion: The version of InfluxDB instances
//
// Returns:
//   - A string containing the rendered Telegraf plugin configuration
func getTelegrafPluginsConfig(configVars map[string]string, influxDBVersion string) string {
	telegrafPluginConf := `[[outputs.cloudwatch]]
  region = "{{.Region}}"
  namespace = "AWS/Timestream/InfluxDB"
  high_resolution_metrics = {{.EnableHighResolutionMetrics}}
  [outputs.cloudwatch.tagpass]
    DbInstanceName = ["{{.InstanceName}}"]

`
	if influxDBVersion == "3" {
		telegrafPluginConf += `[[inputs.prometheus]]
  urls = ["{{.InstanceEndpoint}}"]
  tags = { DbInstanceName = "{{.InstanceName}}" }
  bearer_token_string = "{{.InstanceToken}}"

`
	} else {
		telegrafPluginConf += `[[inputs.prometheus]]
  urls = ["{{.InstanceEndpoint}}"]
  tags = { DbInstanceName = "{{.InstanceName}}" }

`
	}

	telegrafTmpl, err := template.New("telegrafConfigTemplate").Parse(telegrafPluginConf)
	if err != nil {
		log.Printf("Failed creating new template for Telegraf plugin config: %s", err.Error())
		os.Exit(1)
	}

	var templateBuf bytes.Buffer
	if err := telegrafTmpl.Execute(&templateBuf, configVars); err != nil {
		log.Printf("Failed to execute template for Telegraf plugins config: %s", err.Error())
		os.Exit(1)
	}
	return templateBuf.String()
}

// getEc2InitScript generates a bash script that will be used as the EC2 user data
// to set up and configure Telegraf on the instance at launch time.
//
// Parameters:
//   - telegrafConfig: A map containing the complete Telegraf configuration and initialization timestamp
//
// Returns:
//   - A string containing the rendered EC2 initialization script
func getEc2InitScript(telegrafConfig map[string]string) string {

	ec2InitScript := `#!/bin/bash
exec > /tmp/ec2-init.log 2>&1
set -x

# Install GPG key for yum repository
curl -s https://repos.influxdata.com/influxdata-archive_compat.key -o /tmp/influxdata-key.gpg
sudo rpm --import /tmp/influxdata-key.gpg

sudo sh -c 'cat <<EOT >> /etc/yum.repos.d/influxdb.repo
[influxdb]
name = InfluxData Repository - Stable
baseurl = https://repos.influxdata.com/stable/x86_64/main
enabled = 1
gpgcheck = 1
EOT'

# Install Telegraf
sudo yum install telegraf -y

# Backup current configs
sudo mv /etc/telegraf/telegraf.conf /etc/telegraf/telegraf.bckp

# Telegraf Config
sudo sh -c 'cat <<EOT >> /etc/telegraf/telegraf.conf
{{.TelegrafConfig}}
EOT'

sudo sh -c 'cat <<EOT >> /etc/init.d/telegraf
#!/bin/bash
#
# telegraf   Startup script for Telegraf
#
# chkconfig:   2345 95 20
# description: Telegraf is an open-source agent for collecting metrics
#

### BEGIN INIT INFO
# Provides:          telegraf
# Required-Start:    $local_fs $network $remote_fs $syslog
# Required-Stop:     $local_fs $network $remote_fs $syslog
# Should-Start:      $syslog
# Should-Stop:       $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start Telegraf
# Description:       Start the Telegraf service
### END INIT INFO


start() {
    echo "Starting Telegraf..."
    /usr/bin/telegraf --config /etc/telegraf/telegraf.conf &
}

stop() {
    echo "Stopping Telegraf..."
    pkill -f telegraf
}

restart() {
    stop
    start
}

status() {
    ps aux | grep telegraf | grep -v grep
}

case "\$1" in
  start)
      start
      ;;
  stop)
      stop
      ;;
  restart)
      restart
      ;;
  status)
      status
      ;;
  *)
      echo "Usage: \$0 {start|stop|restart|status}"
      exit 1
      ;;
esac
EOT'

# Make telegraf executable
sudo chmod +x /etc/init.d/telegraf

# Setup telegraf to run on boot
sudo chkconfig --add telegraf
sudo chkconfig telegraf on

# Start Telegraf Service
service telegraf start

# Script initialized at {{.Ec2InitTime}}
`
	ec2InitTemplate, err := template.New("ec2InitTemplate").Parse(ec2InitScript)
	if err != nil {
		log.Printf("Failed to create new template for EC2 init script")
		os.Exit(1)
	}

	var ec2InitScriptBuf bytes.Buffer
	if err := ec2InitTemplate.Execute(&ec2InitScriptBuf, telegrafConfig); err != nil {
		log.Printf("Failed to execute template for EC2 init script")
		os.Exit(1)
	}
	return ec2InitScriptBuf.String()
}

// influxDBSecurityGroupRule represents the security group rule configuration
// for an InfluxDB instance, including visibility, port, and security group ID
type influxDBSecurityGroupRule struct {
	influxDBInstanceVisibility bool
	influxDBInstancePort       int32
	influxDBSecurityGroupId    string
}

// addInfluxDBSgRuleInfo adds a security group rule for an InfluxDB instance to a map of rules,
// ensuring that only one rule exists for each unique combination of port and visibility.
//
// Parameters:
//   - influxDBSgRules: A map to store unique security group rules
//   - publiclyAccessible: Whether the InfluxDB instance is publicly accessible
//   - port: The port number of the InfluxDB instance
//   - securityGroupId: The security group ID of the InfluxDB instance
func addInfluxDBSgRuleInfo(influxDBSgRules map[string]influxDBSecurityGroupRule, publiclyAccessible bool, port int32, securityGroupId string) {
	// We only need on rule for each unique combination of port and publicly accessible per InfluxDB instance
	mapKey := fmt.Sprintf("%t:%d", publiclyAccessible, port)
	if _, exists := influxDBSgRules[mapKey]; !exists {
		influxDBSgRules[mapKey] = influxDBSecurityGroupRule{
			influxDBInstanceVisibility: publiclyAccessible,
			influxDBInstancePort:       port,
			influxDBSecurityGroupId:    securityGroupId,
		}
	}
}

// addTelegrafEC2InstanceToStack creates and configures an EC2 instance with Telegraf
// to collect metrics from the specified InfluxDB instances.
//
// Parameters:
//   - stack: The CDK stack to add resources to
//   - stackProps: Properties of the CDK stack
//   - influxDBInstances: Map of InfluxDB instance IDs and optional tokens
//   - influxDBClusters: Map of InfluxDB cluster IDs
//   - telegrafSshCidr: CIDR range for SSH access to the EC2 instance (optional)
//   - ec2Tags: Map of tags for the EC2 instance running Telegraf (optional)
//   - enableHighResolutionMetrics: Whether to enable high resolution metrics collection
//   - influxDBVersion: The verson of InfluxDB instances
//
// Returns:
//   - A comma-separated string of InfluxDB instance name, InfluxDB instance size and InfluxDB instance storage type
//   - An error if any operation fails
func addTelegrafEC2InstanceToStack(stack awscdk.Stack, stackProps awscdk.StackProps, influxDBInstances map[string]string, influxDBClusters map[string]string, telegrafSshCidr string, ec2Tags map[string]string, enableHighResolutionMetrics bool, influxDBVersion string) (string, error) {
	instanceRole := awsiam.NewRole(stack, jsii.String("influxdb-dashboard-ec2-role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	cloudwatchPolicy := awsiam.NewPolicy(stack, jsii.String("influxdb-dashboard-ec2-policy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("cloudwatch:ListMetrics"),
					jsii.String("cloudwatch:GetMetricData"),
					jsii.String("cloudwatch:PutMetricData"),
				},
				Resources: &[]*string{
					jsii.String("*"),
				},
				Conditions: &map[string]interface{}{
					"StringEquals": map[string]*string{
						"cloudwatch:namespace": jsii.String("AWS/Timestream/InfluxDB"),
					},
				},
			}),
		},
	})

	cloudwatchPolicy.AttachToRole(instanceRole)

	influxDBSgRules := make(map[string]influxDBSecurityGroupRule)
	// Comma separate info for each InfluxDB instance in the format:
	// db-instance-name:db-instance-size:db-instance-storage-type,...
	influxDBInstanceInfo := ""
	instanceEndpoint := ""
	vpcId := ""

	var telegrafBaseConfigTemplateVars map[string]string = make(map[string]string)
	if enableHighResolutionMetrics {
		telegrafBaseConfigTemplateVars["Interval"] = "10s"
	} else {
		telegrafBaseConfigTemplateVars["Interval"] = "1m"
	}

	telegrafConfig := getBaseTelegrafConfig(telegrafBaseConfigTemplateVars)

	ctx := context.Background()
	var awsCredentials aws.CredentialsProvider
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("Error loading AWS config: %s", err.Error())
		os.Exit(1)
	}
	awsCredentials = awsConfig.Credentials
	influxDBClient := timestreaminfluxdb.New(timestreaminfluxdb.Options{
		Credentials: awsCredentials,
		Region:      *stack.Region(),
	})
	ec2Client := ec2.New(ec2.Options{
		Credentials: awsCredentials,
		Region:      *stack.Region(),
	})

	// List each db instance in the cluster and add to the influxDBInstances map
	for clusterId, clusterToken := range influxDBClusters {
		clusterInstances, err := influxDBClient.ListDbInstancesForCluster(ctx, &timestreaminfluxdb.ListDbInstancesForClusterInput{
			DbClusterId: &clusterId,
		})
		if err != nil {
			log.Printf("Error listing DB instances for cluster %s: %s", clusterId, err)
			os.Exit(1)
		}

		if influxDBInstances == nil {
			influxDBInstances = make(map[string]string)
		}
		for _, instanceId := range clusterInstances.Items {
			influxDBInstances[*instanceId.Id] = clusterToken
		}
	}

	for instanceId, instanceToken := range influxDBInstances {

		influxDBInstance, err := influxDBClient.GetDbInstance(ctx, &timestreaminfluxdb.GetDbInstanceInput{
			Identifier: &instanceId,
		})

		if err != nil {
			log.Printf("Error describing InfluxDB instance %s: %s", instanceId, err)
			os.Exit(1)
		}
		instanceEndpoint = fmt.Sprintf("https://%s:%d", *influxDBInstance.Endpoint, *influxDBInstance.Port)

		vpc, err := ec2Client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
			SubnetIds: influxDBInstance.VpcSubnetIds,
		})
		if err != nil {
			log.Printf("Failed to describe subnets for InfluxDB instance: %s", err)
			os.Exit(1)
		}

		if vpcId == "" {
			vpcId = *vpc.Subnets[0].VpcId
		} else if vpcId != *vpc.Subnets[0].VpcId {
			log.Printf("Error: all InfluxDB instances must be in the same VPC.")
			os.Exit(1)
		}

		addInfluxDBSgRuleInfo(influxDBSgRules, *influxDBInstance.PubliclyAccessible, *influxDBInstance.Port, influxDBInstance.VpcSecurityGroupIds[0])

		telegrafPluginConfigTemplateVars := map[string]string{
			"InstanceName":                *influxDBInstance.Name,
			"Region":                      *stackProps.Env.Region,
			"InstanceEndpoint":            instanceEndpoint,
			"InstanceToken":               instanceToken,
			"EnableHighResolutionMetrics": "false",
		}

		if enableHighResolutionMetrics {
			telegrafPluginConfigTemplateVars["EnableHighResolutionMetrics"] = "true"
		} else {
			telegrafPluginConfigTemplateVars["EnableHighResolutionMetrics"] = "false"
		}

		telegrafConfig += getTelegrafPluginsConfig(telegrafPluginConfigTemplateVars, influxDBVersion)

		if influxDBInstanceInfo != "" {
			influxDBInstanceInfo += ","
		}
		influxDBInstanceInfo += *influxDBInstance.Name + ":" + string(influxDBInstance.DbInstanceType) + ":" + string(influxDBInstance.DbStorageType)
	}

	ec2InitScript := getEc2InitScript(
		map[string]string{
			"TelegrafConfig": telegrafConfig,
			"Ec2InitTime":    fmt.Sprintf("%d", time.Now().Unix()),
		},
	)

	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("InfluxDBMetricsDashboardVpc"), &awsec2.VpcLookupOptions{
		Region: jsii.String(*stackProps.Env.Region),
		VpcId:  jsii.String(vpcId),
	})

	ec2SecurityGroup := awsec2.NewSecurityGroup(stack, jsii.String("TelegrafEC2SG"), &awsec2.SecurityGroupProps{
		Vpc:               vpc,
		SecurityGroupName: jsii.String("InstanceSecurityGroup"),
		Description:       jsii.String("Allow EC2 access to InfluxDB instances and optional SSH"),
	})

	// Add a rule for each InfluxDB instance in the EC2 security group
	for _, rule := range influxDBSgRules {
		ec2SecurityGroup.AddIngressRule(
			awsec2.Peer_Ipv4(jsii.String(*vpc.VpcCidrBlock())),
			awsec2.Port_Tcp(jsii.Number(rule.influxDBInstancePort)),
			jsii.String("Allow open access to InfluxDB /metrics endpoint"),
			jsii.Bool(false),
		)
	}

	// If a CIDR is supplied for EC2 SSH add a new rule
	if telegrafSshCidr != "" {
		ec2SecurityGroup.AddIngressRule(
			awsec2.Peer_Ipv4(jsii.String(telegrafSshCidr)),
			awsec2.Port_Tcp(jsii.Number(22)),
			jsii.String("Allow SSH access"),
			jsii.Bool(false),
		)
	}

	ec2Instance := awsec2.NewInstance(stack, jsii.String("TelegrafInfluxDBMetricScraper"), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_BURSTABLE2, awsec2.InstanceSize_NANO),
		MachineImage: awsec2.NewAmazonLinuxImage(&awsec2.AmazonLinuxImageProps{
			Generation: awsec2.AmazonLinuxGeneration_AMAZON_LINUX_2023,
		}),
		Vpc:                       vpc,
		UserData:                  awsec2.UserData_Custom(jsii.String(ec2InitScript)),
		Role:                      instanceRole,
		SecurityGroup:             ec2SecurityGroup,
		UserDataCausesReplacement: jsii.Bool(true),
	})

	// Add any provided context tags to the EC2 instance
	for tagKey, tagVal := range ec2Tags {
		awscdk.Tags_Of(ec2Instance).Add(
			jsii.String(tagKey),
			jsii.String(tagVal),
			&awscdk.TagProps{},
		)
	}

	// Add a rule for each InfluxDB instance with EC2 access
	for i, rule := range influxDBSgRules {
		var targetIp *string
		if rule.influxDBInstanceVisibility {
			targetIp = ec2Instance.InstancePublicIp()
		} else {
			targetIp = ec2Instance.InstancePrivateIp()
		}

		influxDBSecurityGroup := awsec2.SecurityGroup_FromSecurityGroupId(
			stack,
			jsii.String("InfluxDBSG"+i),
			jsii.String(rule.influxDBSecurityGroupId),
			&awsec2.SecurityGroupImportOptions{
				Mutable: jsii.Bool(true),
			},
		)

		influxDBSecurityGroup.AddIngressRule(
			awsec2.Peer_Ipv4(jsii.String(*targetIp+"/32")),
			awsec2.Port_Tcp(jsii.Number(rule.influxDBInstancePort)),
			jsii.String("Allow EC2 instance access to InfluxDB instances"),
			jsii.Bool(false),
		)
	}

	awscdk.NewCfnOutput(stack, jsii.String("EC2 Instance ID"), &awscdk.CfnOutputProps{
		Value:       ec2Instance.InstanceId(),
		Description: jsii.String("The instance ID of the EC2 instance running Telegraf"),
	})

	return influxDBInstanceInfo, nil
}

// addGrafanaWorkspaceToStack creates and configures an Amazon Managed Grafana workspace
// with the necessary permissions to access CloudWatch metrics.
//
// Parameters:
//   - stack: The CDK stack to add resources to
//   - stackProps: Properties of the CDK stack
//   - grafanaWorkspaceName: The name for the Grafana workspace
//   - grafanaWorkspaceTags: Map of tags for the Grafana workspace
//
// Returns:
//   - The updated CDK stack
//   - An error if any operation fails
func addGrafanaWorkspaceToStack(stack awscdk.Stack, stackProps awscdk.StackProps, grafanaWorkspaceName string, grafanaWorkspaceTags map[string]string) (awscdk.Stack, error) {
	workspacePolicy := awsiam.NewPolicy(stack, jsii.String("influxdb-dashboard-workspace-policy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("cloudwatch:GetMetricData"),
					jsii.String("cloudwatch:GetMetricStatistics"),
					jsii.String("cloudwatch:ListMetrics"),
				},
				Resources: &[]*string{
					jsii.String("*"), // Cannot refine futher due to Grafana functionality
				},
			}),
		},
	})

	workspaceRole := awsiam.NewRole(stack, jsii.String("influxdb-dashboard-workspace-role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("grafana.amazonaws.com"), nil),
	})

	workspacePolicy.AttachToRole(workspaceRole)

	grafanaWorkspace := awsgrafana.NewCfnWorkspace(stack, jsii.String(grafanaWorkspaceName), &awsgrafana.CfnWorkspaceProps{
		AccountAccessType:       jsii.String("CURRENT_ACCOUNT"),
		AuthenticationProviders: &[]*string{jsii.String("AWS_SSO")},
		PermissionType:          jsii.String("CUSTOMER_MANAGED"),
		PluginAdminEnabled:      true,
		RoleArn:                 workspaceRole.RoleArn(),
		Name:                    jsii.String(grafanaWorkspaceName),
	})

	// Add any provided context tags to the Grafana workspace
	for tagKey, tagVal := range grafanaWorkspaceTags {
		awscdk.Tags_Of(grafanaWorkspace).Add(
			jsii.String(tagKey),
			jsii.String(tagVal),
			&awscdk.TagProps{},
		)
	}

	workspaceUri := "https://" + *grafanaWorkspace.AttrEndpoint()
	awscdk.NewCfnOutput(stack, jsii.String("Grafana Workspace URL"), &awscdk.CfnOutputProps{
		Value:       &workspaceUri,
		Description: jsii.String("The URI of the Grafana workspace"),
	})

	return stack, nil
}

// createLambdaResource creates a Lambda function and custom resource to deploy
// a Grafana dashboard for InfluxDB metrics after stack creation.
//
// Parameters:
//   - stack: The CDK stack to add resources to
//   - stackProps: Properties of the CDK stack
//   - grafanaWorkspaceName: The name of the Grafana workspace
//   - dashboardName: The name for the Grafana dashboard
//   - dbClusterInfo: Comma-separated list of InfluxDB instance name, instance size, and instance storage type
//   - dashboardDataGranularity: The granularity of the dashboard used, default is 60s and fine granularity is 5s
//   - influxDBVersion: The version of InfluxDB instances
//
// Returns:
//   - The updated CDK stack
//   - An error if any operation fails
func createLambdaResource(stack awscdk.Stack, stackProps awscdk.StackProps, grafanaWorkspaceName string, dashboardName string, dbClusterInfo string, dashboardDataGranularity string, influxDBVersion string) (awscdk.Stack, error) {
	var lambdaTimeout float64 = 600.0

	lambdaHandler := awslambda.NewFunction(stack, jsii.String("influxDBMetricDashboardLambdaHandler"), &awslambda.FunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2023(),
		Architecture: awslambda.Architecture_ARM_64(),
		Handler:      jsii.String("main"),
		Environment: &map[string]*string{
			"GOARCH":                   jsii.String("arm64"),
			"GOOS":                     jsii.String("linux"),
			"GrafanaWorkspaceName":     jsii.String(grafanaWorkspaceName),
			"DashboardName":            jsii.String(dashboardName),
			"DbClusterInfo":            jsii.String(dbClusterInfo),
			"dashboardDataGranularity": jsii.String(dashboardDataGranularity),
			"influxDBVersion":          jsii.String(influxDBVersion),
		},
		Code: awslambda.Code_FromCustomCommand(jsii.String("lambda/upload_dashboard/lambda.zip"), &[]*string{
			jsii.String("go"),
			jsii.String("run"),
			jsii.String("lambda/upload_dashboard/bundle.go"),
		}, nil),
		Timeout: awscdk.Duration_Seconds(&lambdaTimeout),
		InitialPolicy: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("grafana:ListWorkspaces"),
				},
				Resources: &[]*string{
					jsii.String(fmt.Sprintf("arn:aws:grafana:%s:%s:/workspaces", *stackProps.Env.Region, *stackProps.Env.Account)),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("grafana:CreateWorkspaceServiceAccount"),
					jsii.String("grafana:CreateWorkspaceServiceAccountToken"),
					jsii.String("grafana:DeleteWorkspaceServiceAccountToken"),
					jsii.String("grafana:ListWorkspaceServiceAccounts"),
					jsii.String("grafana:ListWorkspaceServiceAccountTokens"),
				},
				Resources: &[]*string{
					jsii.String(fmt.Sprintf("arn:aws:grafana:%s:%s:/workspaces/*", *stackProps.Env.Region, *stackProps.Env.Account)),
				},
			}),
		},
	})

	customResourceProvider := customresources.NewProvider(stack, jsii.String("LambdaCustomResourceProvider"), &customresources.ProviderProps{
		OnEventHandler: lambdaHandler,
	})

	// The timestamp is arbitrary, but will trigger execution for each cdk app deployment
	customResourceProperties := map[string]interface{}{
		"Trigger": fmt.Sprintf("%d", time.Now().Unix()),
	}

	// Custom Resource that triggers the Lambda function after stack creation
	awscdk.NewCustomResource(stack, jsii.String("UploadDashboardCustomResource"), &awscdk.CustomResourceProps{
		ServiceToken: customResourceProvider.ServiceToken(),
		Properties:   &customResourceProperties,
	})

	return stack, nil
}

// parseKeyValueContext parses context parameters in the format "key1:val1,Key2:val2" and returns the associated map.
//
// Parameters:
//   - contextStr: String map to parse
//
// Returns:
//   - Map of parsed parameters
func parseKeyValueContext(contextStr string) map[string]string {
	tagsMap := make(map[string]string)
	tagPairs := strings.Split(contextStr, ",")
	for _, pair := range tagPairs {
		keyVal := strings.SplitN(pair, ":", 2)
		if len(keyVal) == 2 {
			tagKey := strings.TrimSpace(keyVal[0])
			tagValue := strings.TrimSpace(keyVal[1])
			tagsMap[tagKey] = tagValue
		} else if len(keyVal) == 1 {
			tagKey := strings.TrimSpace(keyVal[0])
			tagsMap[tagKey] = ""
		} else {
			log.Printf("Failed to parse context tags, ensure you are using the format \"key1:val1,Key2:val2\", or \"key1,key2\". Failed tags string: %s", contextStr)
			os.Exit(1)
		}
	}
	return tagsMap
}

// Create and configures the CDK stack for monitoring InfluxDB instances with Telegraf, CloudWatch, and Grafana.
func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)
	stackId := "InfluxDBMetricsDashboard"
	var stackProps awscdk.StackProps = awscdk.StackProps{Env: env()}
	stack := awscdk.NewStack(app, &stackId, &stackProps)

	// Need to use context to access variables at time of App Synthesization
	// Required context
	influxDBVersionContext := stack.Node().TryGetContext(jsii.String("InfluxDBVersion"))
	if influxDBVersionContext == nil {
		log.Printf("InfluxDBVersion context is required")
		os.Exit(1)
	}
	if influxDBVersionContext.(string) != "2" && influxDBVersionContext.(string) != "3" {
		log.Printf("InfluxDBVersion must be either \"2\" or \"3\"")
		os.Exit(1)
	}

	influxDBClusterContext := stack.Node().TryGetContext(jsii.String("InfluxDBClusterIds"))
	influxDBInstanceContext := stack.Node().TryGetContext(jsii.String("InfluxDBIds"))
	var influxDBInstances map[string]string
	var influxDBClusters map[string]string

	if influxDBInstanceContext == nil && influxDBClusterContext == nil {
		log.Printf("InfluxDBIds or InfluxDBClusters context is required to scrape metric endpoints")
		os.Exit(1)
	}

	if influxDBInstanceContext != nil {
		influxDBInstances = parseKeyValueContext(influxDBInstanceContext.(string))
		if influxDBVersionContext.(string) == "3" {
			for instanceId, instanceToken := range influxDBInstances {
				if instanceToken == "" {
					log.Printf("No token provided for db instance %s. A token value must be provided for each database instance for version 3 instances.", instanceId)
					os.Exit(1)
				}
			}
		}
	}

	if influxDBClusterContext != nil {
		influxDBClusters = parseKeyValueContext(influxDBClusterContext.(string))
		if influxDBVersionContext.(string) == "3" {
			for clusterId, clusterToken := range influxDBClusters {
				if clusterToken == "" {
					log.Printf("No token provided for db cluster %s. A token value must be provided for each cluster for version 3 clusters.", clusterId)
					os.Exit(1)
				}
			}
		}
	}

	// Optional context
	grafanaWorkspaceNameContext := stack.Node().TryGetContext(jsii.String("GrafanaWorkspaceName"))
	grafanaWorkspaceName := "InfluxDBMetricDashboardWorkspace"
	if grafanaWorkspaceNameContext != nil {
		grafanaWorkspaceName = grafanaWorkspaceNameContext.(string)
	}
	dashboardNameContext := stack.Node().TryGetContext(jsii.String("DashboardName"))
	dashboardName := "InfluxDB Performance Dashboard"
	if dashboardNameContext != nil {
		dashboardName = dashboardNameContext.(string)
	}
	telegrafSshCidrContext := stack.Node().TryGetContext(jsii.String("TelegrafSshCidr"))
	telegrafSshCidr := ""
	if telegrafSshCidrContext != nil {
		telegrafSshCidr = telegrafSshCidrContext.(string)
		_, _, err := net.ParseCIDR(telegrafSshCidr)
		if err != nil {
			log.Printf("The value provided for TelegrafSshCidr context is not a valid CIDR range.")
			os.Exit(1)
		}
	}
	dashboardDataGranularity := "60s"
	enableHighResolutionMetricsContext := stack.Node().TryGetContext(jsii.String("EnableHighResolutionMetrics"))
	enableHighResolutionMetrics := false
	if enableHighResolutionMetricsContext != nil && enableHighResolutionMetricsContext.(string) == "true" {
		enableHighResolutionMetrics = true
		dashboardDataGranularity = "10s"
	}
	ec2InstanceTagsContext := stack.Node().TryGetContext(jsii.String("TelegrafEc2Tags"))
	var ec2InstanceTags map[string]string
	if ec2InstanceTagsContext != nil {
		ec2InstanceTags = parseKeyValueContext(ec2InstanceTagsContext.(string))
	}
	grafanaWorkspaceTagsContext := stack.Node().TryGetContext(jsii.String("GrafanaWorkspaceTags"))
	var grafanaWorkspaceTags map[string]string = nil
	if grafanaWorkspaceTagsContext != nil {
		grafanaWorkspaceTags = parseKeyValueContext(grafanaWorkspaceTagsContext.(string))
	}

	influxDBClusterInfo, err := addTelegrafEC2InstanceToStack(stack, stackProps, influxDBInstances, influxDBClusters, telegrafSshCidr, ec2InstanceTags, enableHighResolutionMetrics, influxDBVersionContext.(string))
	if err != nil {
		log.Printf("Error adding Telegraf instance to stack: %s", err)
		return
	}

	_, err = addGrafanaWorkspaceToStack(stack, stackProps, grafanaWorkspaceName, grafanaWorkspaceTags)
	if err != nil {
		log.Printf("Error adding Grafana workspace to stack: %s", err)
		return
	}
	_, err = createLambdaResource(stack, stackProps, grafanaWorkspaceName, dashboardName, influxDBClusterInfo, dashboardDataGranularity, influxDBVersionContext.(string))
	if err != nil {
		log.Printf("Error adding Lambda function to stack: %s", err)
		return
	}

	app.Synth(nil)
}

// env returns the AWS environment (account and region) for the CDK stack
//
// Returns:
//   - A pointer to an awscdk.Environment with account and region information
func env() *awscdk.Environment {
	return &awscdk.Environment{
		Account: jsii.String(os.Getenv("CDK_DEFAULT_ACCOUNT")),
		Region:  jsii.String(os.Getenv("CDK_DEFAULT_REGION")),
	}
}
