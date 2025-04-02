package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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

func addTelegrafEC2InstanceToStack(stack awscdk.Stack, stackProps awscdk.StackProps, databaseName string, influxDBIds string) (awscdk.Stack, error) {
	instanceRole := awsiam.NewRole(stack, jsii.String("influxdb-dashboard-ec2-role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	timestreamPolicy := awsiam.NewPolicy(stack, jsii.String("influxdb-dashboard-ec2-policy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("timestream:DescribeDatabase"),
				},
				Resources: &[]*string{
					jsii.String(fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s", *stackProps.Env.Region, *stackProps.Env.Account, databaseName)),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("timestream:WriteRecords"),
					jsii.String("timestream:CreateTable"),
					jsii.String("timestream:DescribeTable"),
				},
				Resources: &[]*string{
					jsii.String(fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s/table/*", *stackProps.Env.Region, *stackProps.Env.Account, databaseName)),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("timestream:DescribeEndpoints"),
					jsii.String("cloudwatch:ListMetrics"),
					jsii.String("cloudwatch:GetMetricData"),
				},
				Resources: &[]*string{
					jsii.String("*"),
				},
			}),
		},
	})

	timestreamPolicy.AttachToRole(instanceRole)

	ctx := context.Background()
	var awsCredentials aws.CredentialsProvider
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("Error loading AWS config: " + err.Error())
		os.Exit(1)
	}
	awsCredentials = awsConfig.Credentials
	svc := timestreaminfluxdb.New(timestreaminfluxdb.Options{
		Credentials: awsCredentials,
		Region:      *stack.Region(),
	})
	ec2svc := ec2.New(ec2.Options{
		Credentials: awsCredentials,
		Region:      *stack.Region(),
	})

	vpcId := ""
	telegrafInputOutputConfig := ""
	instanceEndpoint := ""

	// Split the comma separated list of Ids
	influxDBIdArr := strings.Split(influxDBIds, ",")
	for _, instanceId := range influxDBIdArr {

		influxDBInstance, err := svc.GetDbInstance(ctx, &timestreaminfluxdb.GetDbInstanceInput{
			Identifier: &instanceId,
		})
		if err != nil {
			log.Printf("Error describing InfluxDB instance: %s", err)
			os.Exit(1)
		}
		instanceEndpoint = fmt.Sprintf("https://%s:%d", *influxDBInstance.Endpoint, *influxDBInstance.Port)

		vpc, err := ec2svc.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
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

		telegrafInputOutputConfig += fmt.Sprintf(`
[[outputs.timestream]]
  region = "%s"
  database_name = "%s"
  describe_database_on_start = false
  mapping_mode = "multi-table"
  measure_name_for_multi_measure_records = "telegraf_measure"
  use_multi_measure_records = true
  create_table_if_not_exists = true
  create_table_magnetic_store_retention_period_in_days = 365
  create_table_memory_store_retention_period_in_hours = 24
  [outputs.timestream.tagpass]
    influxDBInstance = ["%s"]
[[inputs.prometheus]]
  urls = ["%s"]
  tags = { influxDBInstance = "%s" }
[[inputs.cloudwatch]]
  region = "%s"
  period = "5m"
  delay = "5m"
  interval = "5m"
  namespaces = ["AWS/Timestream/InfluxDB"]
  tags = { influxDBInstance = "%s" }
  [[inputs.cloudwatch.metrics]]
    names = ["DiskUtilization", "CPUUtilization", "MemoryUtilization"]
    [[inputs.cloudwatch.metrics.dimensions]]
      name = "DbInstanceName"
      value = "%s"

      `, *stackProps.Env.Region, databaseName, instanceId, instanceEndpoint, instanceId, *stackProps.Env.Region, instanceId, *influxDBInstance.Name)
	}

	userDataScript :=
		fmt.Sprintf(`
#!/bin/bash
cat <<EOT >> /etc/yum.repos.d/influxdb.repo
[influxdb]
name = InfluxData Repository - Stable
baseurl = https://repos.influxdata.com/stable/\$basearch/main
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOT

# Install Telegraf
yum install telegraf -y

# Backup current configs
mv /etc/telegraf/telegraf.conf /etc/telegraf/telegraf.bckp

# Timestream Config
cat <<EOT >> /etc/telegraf/telegraf.conf
[global_tags]
  microservice = "web"
  region = "%s"

[agent]
  interval = "10s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
  flush_jitter = "0s"
  precision = ""
  hostname = ""
  omit_hostname = false
%s
EOT

cat <<EOT >> /etc/init.d/telegraf
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
EOT

# Make telegraf executable
sudo chmod +x /etc/init.d/telegraf

# Setup telegraf to run on boot
sudo chkconfig --add telegraf
sudo chkconfig telegraf on

# Start Telegraf Service
service telegraf start
`, *stackProps.Env.Region, telegrafInputOutputConfig)

	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("InfluxDBMetricsDashboardVpc"), &awsec2.VpcLookupOptions{
		Region: jsii.String(*stackProps.Env.Region),
		VpcId:  jsii.String(vpcId),
	})
	ec2SecurityGroup := awsec2.NewSecurityGroup(stack, jsii.String("TelegrafEC2SG"), &awsec2.SecurityGroupProps{
		Vpc:               vpc,
		SecurityGroupName: jsii.String("InstanceSecurityGroup"),
		Description:       jsii.String("Allow SSH and all outbound traffic"),
	})
	ec2SecurityGroup.AddIngressRule(
		awsec2.Peer_Ipv4(jsii.String("0.0.0.0/0")),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow SSH access"),
		jsii.Bool(false),
	)
	ec2SecurityGroup.AddIngressRule(
		awsec2.Peer_Ipv4(jsii.String("0.0.0.0/0")),
		awsec2.Port_Tcp(jsii.Number(8086)),
		jsii.String("Allow open access to InfluxDB /metrics endpoint"),
		jsii.Bool(false),
	)

	ec2Instance := awsec2.NewInstance(stack, jsii.String("TelegrafInfluxDBMetricScraper"), &awsec2.InstanceProps{
		InstanceType:  awsec2.InstanceType_Of(awsec2.InstanceClass_BURSTABLE2, awsec2.InstanceSize_NANO),
		MachineImage:  awsec2.NewAmazonLinuxImage(&awsec2.AmazonLinuxImageProps{}),
		Vpc:           vpc,
		UserData:      awsec2.UserData_Custom(jsii.String(userDataScript)),
		Role:          instanceRole,
		SecurityGroup: ec2SecurityGroup,
	})

	awscdk.NewCfnOutput(stack, jsii.String("EC2 Instance ID"), &awscdk.CfnOutputProps{
		Value:       ec2Instance.InstanceId(),
		Description: jsii.String("The instance ID of the EC2 instance running Telegraf"),
	})

	return stack, nil
}

func addGrafanaWorkspaceToStack(stack awscdk.Stack, stackProps awscdk.StackProps, databaseName string, grafanaWorkspaceName string) (awscdk.Stack, error) {
	workspacePolicy := awsiam.NewPolicy(stack, jsii.String("influxdb-dashboard-workspace-policy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("timestream:DescribeEndpoints"),
				},
				Resources: &[]*string{
					jsii.String("*"),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Actions: &[]*string{
					jsii.String("timestream:Select"),
				},
				Resources: &[]*string{
					jsii.String(fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s/table/*", *stackProps.Env.Region, *stackProps.Env.Account, databaseName)),
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

	workspaceUri := "https://" + *grafanaWorkspace.AttrEndpoint()
	awscdk.NewCfnOutput(stack, jsii.String("Grafana Workspace URL"), &awscdk.CfnOutputProps{
		Value:       &workspaceUri,
		Description: jsii.String("The URI of the Grafana workspace"),
	})

	return stack, nil
}

func createLambdaResource(stack awscdk.Stack, stackProps awscdk.StackProps, databaseName string, grafanaWorkspaceName string, dashboardName string, timestreamDatasourceName string) (awscdk.Stack, error) {
	var lambdaTimeout float64 = 200.0

	lambdaHandler := awslambda.NewFunction(stack, jsii.String("influxDBMetricDashboardLambdaHandler"), &awslambda.FunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2023(),
		Architecture: awslambda.Architecture_ARM_64(),
		Handler:      jsii.String("main"),
		Environment: &map[string]*string{
			"GOARCH":                   jsii.String("arm64"),
			"GOOS":                     jsii.String("linux"),
			"GrafanaWorkspaceName":     jsii.String(grafanaWorkspaceName),
			"TimestreamDatasourceName": jsii.String(timestreamDatasourceName),
			"DashboardName":            jsii.String(dashboardName),
			"DatabaseName":             jsii.String(databaseName),
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

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)
	stackId := "InfluxDBMetricsDashboard"
	var stackProps awscdk.StackProps = awscdk.StackProps{Env: env()}
	stack := awscdk.NewStack(app, &stackId, &stackProps)

	// Need to use context to access variables at time of App Synthesization
	// Required context
	influxDBIdContext := stack.Node().TryGetContext(jsii.String("InfluxDBIds"))
	if influxDBIdContext == nil {
		log.Printf("InfluxDBIds context is required to scrape metric endpoints")
		os.Exit(1)
	}

	// Optional context
	grafanaWorkspaceNameContext := stack.Node().TryGetContext(jsii.String("GrafanaWorkspaceName"))
	grafanaWorkspaceName := "InfluxDBMetricDashboardWorkspace"
	if grafanaWorkspaceNameContext != nil {
		grafanaWorkspaceName = grafanaWorkspaceNameContext.(string)
	}
	timestreamDatasourceNameContext := stack.Node().TryGetContext(jsii.String("TimestreamDatasourceName"))
	timestreamDatasourceName := "Amazon Timestream for LiveAnalytics Sample Data Source"
	if timestreamDatasourceNameContext != nil {
		timestreamDatasourceName = timestreamDatasourceNameContext.(string)
	}
	dashboardNameContext := stack.Node().TryGetContext(jsii.String("DashboardName"))
	dashboardName := "InfluxDB Performance Dashboard"
	if dashboardNameContext != nil {
		dashboardName = dashboardNameContext.(string)
	}
	databaseNameContext := stack.Node().TryGetContext(jsii.String("DatabaseName"))
	databaseName := "InfluxDBMetrics"
	if databaseNameContext != nil {
		databaseName = databaseNameContext.(string)
	}

	stack, err := addTelegrafEC2InstanceToStack(stack, stackProps, databaseName, influxDBIdContext.(string))
	if err != nil {
		log.Printf("Error adding Telegraf instance to stack: %s", err)
		return
	}
	stack, err = addGrafanaWorkspaceToStack(stack, stackProps, databaseName, grafanaWorkspaceName)
	if err != nil {
		log.Printf("Error adding Grafana workspace to stack: %s", err)
		return
	}
	stack, err = createLambdaResource(stack, stackProps, databaseName, grafanaWorkspaceName, dashboardName, timestreamDatasourceName)
	if err != nil {
		log.Printf("Error adding Lambda function to stack: %s", err)
		return
	}

	app.Synth(nil)
}

func env() *awscdk.Environment {
	return &awscdk.Environment{
		Account: jsii.String(os.Getenv("CDK_DEFAULT_ACCOUNT")),
		Region:  jsii.String(os.Getenv("CDK_DEFAULT_REGION")),
	}
}
