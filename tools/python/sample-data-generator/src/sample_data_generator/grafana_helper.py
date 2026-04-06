# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from boto3 import session
from botocore.exceptions import ClientError
import json
import time
import requests

# The maximum total number of seconds to wait for a Grafana resource to finish creating.
MAX_WAIT_SECONDS = 900 # 15 minutes
# The number of seconds to wait before checking whether a Grafana resource has finished creating.
WAIT_PERIOD_SECONDS = 15

def create_grafana_workspace(session: session, workspace_name: str, role_name: str, database_name: str, table_name: str) -> str:
    """
    Creates an Amazon Managed Grafana workspace.

    :param session: session: The AWS session to use for clients.
    :param workspace_name: str: The name to use for the workspace.
    :param role_name: str: The name to use when creating the workspace's IAM role.
    :param database_name: str: The Timestream for LiveAnalytics database the workspace will use.
    :param table_name: str: The Timestream for LiveAnalytics table the workspace will use.
    :returns: The workspace ID.
    """
    grafana_client = session.client('grafana')

    workspace_role_arn = create_grafana_workspace_role(session, role_name, database_name, table_name)

    workspace_id = ""
    try:
        list_workspaces_response = grafana_client.list_workspaces()
        for workspace in list_workspaces_response['workspaces']:
            if workspace['name'] == workspace_name:
                print(f"Workspace '{workspace_name}' already exists with ID: {workspace['id']}")
                workspace_id = workspace['id']
                current_wait_seconds = 0
                if workspace['status'] != 'ACTIVE':
                    status = ""
                    while current_wait_seconds < MAX_WAIT_SECONDS:
                        status = grafana_client.describe_workspace(workspaceId=workspace_id)['workspace']['status']
                        print(f"Workspace status: {status}")
                        if status == 'ACTIVE':
                            break
                        time.sleep(WAIT_PERIOD_SECONDS)
                        current_wait_seconds += WAIT_PERIOD_SECONDS
                    if current_wait_seconds >= MAX_WAIT_SECONDS and status != 'ACTIVE':
                        raise Exception("Timed out while waiting for workspace to become active")

    except ClientError as e:
        raise Exception(f"Error checking for workspace: {e}")

    if not workspace_id:
        try:
            configuration = {
                "plugins": {
                    "pluginAdminEnabled": True,
                }
            }
            create_workspace_response = grafana_client.create_workspace(
                accountAccessType='CURRENT_ACCOUNT',
                authenticationProviders=['AWS_SSO'],
                permissionType='CUSTOMER_MANAGED',
                workspaceName=workspace_name,
                workspaceRoleArn=workspace_role_arn,
                configuration=json.dumps(configuration)
            )
        except Exception as err:
            print(f"Failed to create workspace: {err}")
            raise
        else:
            workspace_id = create_workspace_response['workspace']['id']
            print(f"Workspace '{workspace_name}' created with ID: {workspace_id}")

            # Wait until the workspace is active
            current_wait_seconds = 0
            while current_wait_seconds < MAX_WAIT_SECONDS:
                status = grafana_client.describe_workspace(workspaceId=workspace_id)['workspace']['status']
                print(f"Workspace status: {status}")
                if status == 'ACTIVE':
                    break
                time.sleep(WAIT_PERIOD_SECONDS)
                current_wait_seconds += WAIT_PERIOD_SECONDS

            if current_wait_seconds >= MAX_WAIT_SECONDS and status != 'ACTIVE':
                raise Exception("Timed out while waiting for workspace to become active")

    grafana_workspace = grafana_client.describe_workspace(workspaceId=workspace_id)
    return grafana_workspace['workspace']['id']

def create_grafana_workspace_role(session: session, workspace_role_name: str, database_name: str, table_name: str) -> str:
    """
    Creates an IAM role to be used by an Amazon Managed Grafana workspace.

    :param session: session: The AWS session to use for clients.
    :param workspace_role_name: str: The name to use when creating the workspace role.
    :param database_name: str: The Timestream for LiveAnalytics database the workspace will use.
    :param table_name: str: The Timestream for LiveAnalytics table the workspace will use.
    :returns: The workspace IAM role ARN.
    """
    iam_client = session.client('iam')
    sts_client = session.client('sts')

    account_id = account_id = sts_client.get_caller_identity()['Account']

    workspace_role_arn = ""

    # Define the trust policy for Amazon Managed Grafana
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "grafana.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        # Create the IAM role
        create_role_response = iam_client.create_role(
            RoleName=workspace_role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Amazon Managed Grafana to access AWS resources"
        )
        workspace_role_arn = create_role_response['Role']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"Workspace IAM role {workspace_role_name} already exists")
        try:
            workspace_role_arn = iam_client.get_role(RoleName=workspace_role_name)['Role']['Arn']
        except iam_client.exceptions.NoSuchEntityException:
            print("Workspace IAM role could not be found")
            raise
        
    print(f"Created workspace role with ARN: {workspace_role_arn}")

    # Define an inline policy for Timestream and CloudWatch read access
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:DescribeEndpoints",
                    "timestream:ListDatabases"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:Select"
                ],
                "Resource": f"arn:aws:timestream:{session.region_name}:{account_id}:database/{database_name}/table/{table_name}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:ListTables"
                ],
                "Resource": f"arn:aws:timestream:{session.region_name}:{account_id}:database/{database_name}"
            }
        ]
    }

    try:
        # Attach the inline policy
        iam_client.put_role_policy(
            RoleName=workspace_role_name,
            PolicyName='GrafanaWorkspaceAccessPolicy',
            PolicyDocument=json.dumps(inline_policy)
        )
        print(f"Attached inline policy to role {workspace_role_name}")
    except Exception as err:
        print("Failed to attach policy to workspace role")
        raise

    return workspace_role_arn

def create_grafana_workspace_token(session: session, workspace_id: str) -> str:
    """
    Creates and returns a Grafana workspace service account token.

    :param session: session: The AWS session to use for clients.
    :param workspace_id: str: The ID of the workspace.
    :returns: The workspace service account token.
    """
    grafana_client = session.client('grafana')

    workspace_service_account_name = 'admin'
    workspace_service_account_id = ''
    service_account_token = ''
    try:
        create_service_account_response = grafana_client.create_workspace_service_account(grafanaRole='ADMIN', name='admin', workspaceId=workspace_id)
        workspace_service_account_id = create_service_account_response['id']
    except grafana_client.exceptions.ConflictException:
        print("Using existing workspace service account")
        list_workspace_services_accounts_response = grafana_client.list_workspace_service_accounts(
            maxResults=200,
            workspaceId=workspace_id
        )
        next_token = list_workspace_services_accounts_response.get('nextToken', '')
        for service_account in list_workspace_services_accounts_response['serviceAccounts']:
            if service_account['name'] == workspace_service_account_name:
                workspace_service_account_id = service_account['id']
        if not workspace_service_account_id:
            while next_token:
                list_workspace_services_accounts_response = grafana_client.list_workspace_service_accounts(
                    maxResults=200,
                    workspaceId=workspace_id,
                    nextToken=next_token
                )
                for service_account in list_workspace_services_accounts_response['serviceAccounts']:
                    if service_account['name'] == workspace_service_account_name:
                        workspace_service_account_id = service_account['id']
                if workspace_service_account_id:
                    break
                next_token = list_workspace_services_accounts_response.get('nextToken', '')
        if not workspace_service_account_id:
            raise Exception(f"Existing workspace service account with name {workspace_service_account_name} could not be found")
    except Exception as err:
        print(f"An unexpected exception occurred when creating workspace service account: {err}")
        raise

    service_account_token_name = 'admin_token'
    # If the token already exists, it must be deleted and recreated. list_workspace_service_account_tokens
    # will not return its key.
    try:
        service_account_token_id = ''
        list_service_account_tokens_response = grafana_client.list_workspace_service_account_tokens(
            maxResults=200,
            workspaceId=workspace_id,
            serviceAccountId=workspace_service_account_id
        )
        next_token = list_service_account_tokens_response.get("nextToken", '')
        for service_account_token in list_service_account_tokens_response['serviceAccountTokens']:
            if service_account_token['name'] == service_account_token_name:
                service_account_token_id = service_account_token['id']
        if not workspace_service_account_id:
            while next_token:
                list_service_account_tokens_response = grafana_client.list_workspace_service_account_tokens(
                    maxResults=200,
                    workspaceId=workspace_id,
                    nextToken=next_token,
                    serviceAccountId=workspace_service_account_id
                )
                for service_account_token in list_service_account_tokens_response['serviceAccountTokens']:
                    if service_account_token['name'] == service_account_token_name:
                        service_account_token_id = service_account_token['id']
                if service_account_token_id:
                    break
                next_token = list_service_account_tokens_response.get('nextToken', '')
        if service_account_token_id:
            grafana_client.delete_workspace_service_account_token(
                serviceAccountId=workspace_service_account_id,
                tokenId=service_account_token_id,
                workspaceId=workspace_id
            )
    except Exception as err:
        print(f"An unexpected exception occurred when checking for existing service tokens: {err}")
        raise

    try:
        create_token_response = grafana_client.create_workspace_service_account_token(
            name=service_account_token_name,
            secondsToLive=86400, # 1 day
            serviceAccountId=workspace_service_account_id,
            workspaceId=workspace_id
        )
        service_account_token = create_token_response['serviceAccountToken']['key']
    except Exception as err:
        print(f"An exception occurred when trying to create a new service token: {err}")
        raise
    return service_account_token

def get_grafana_workspace_url(session: session, workspace_id: str) -> str:
    """
    Retrieves an Amazon Managed Grafana workspace endpoint.

    :param session: session: The AWS session to use for clients.
    :param workspace_id: str: The workspace ID.
    :returns: The Amazon Managed Grafana workspace's endpoint.
    """
    grafana_client = session.client('grafana')
    return grafana_client.describe_workspace(workspaceId=workspace_id)['workspace']['endpoint']

def add_timestream_plugin(service_account_token: str, grafana_endpoint_url: str):
    """
    Adds the Timestream plugin to a Grafana workspace.

    :param service_account_token: str: The Grafana workspace service account token to use to make requests.
    :param grafana_endpoint_url: str: The endpoint of the Grafana workspace.
    """
    headers = {
        "Authorization": f"Bearer {service_account_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    install_timestream_plugin_response = requests.post(
        f"https://{grafana_endpoint_url}/api/plugins/grafana-timestream-datasource/install",
        headers=headers,
    )

    if install_timestream_plugin_response.status_code == 409:
        print("Amazon Timestream plugin already installed")
    elif not install_timestream_plugin_response.ok:
        raise Exception(f"Failed to install Amazon Timestream plugin for workspace: {install_timestream_plugin_response.content}")

    current_wait_seconds = 0
    while current_wait_seconds < MAX_WAIT_SECONDS:
        installed_plugins_response = requests.get(
            f"https://{grafana_endpoint_url}/api/plugins",
            headers=headers
        )
        if installed_plugins_response.ok:
            installed_plugins_response_json = installed_plugins_response.json()
            if any(installed_plugin.get('id') == 'grafana-timestream-datasource' for installed_plugin in installed_plugins_response_json):
                # Grafana will report the plugin as installed but needs more time
                # for the installation to truly finish
                time.sleep(WAIT_PERIOD_SECONDS)
                print("Amazon Timestream plugin installed")
                break
        else:
            raise Exception("Failed to check currently installed plugins")
        print("Waiting for the Amazon Timestream plugin to finish installing . . .")
        time.sleep(WAIT_PERIOD_SECONDS)
        current_wait_seconds += WAIT_PERIOD_SECONDS

    enable_plugin_payload = {
        "enabled": True,
        "pinned": True,
        "json": None
    }

    # Post request to add the data source
    add_timestream_plugin_response = requests.post(
        f"https://{grafana_endpoint_url}/api/plugins/grafana-timestream-datasource/settings",
        headers=headers,
        data=json.dumps(enable_plugin_payload)
    )

    if add_timestream_plugin_response.ok:
        print("Amazon Timestream plugin enabled")
    elif add_timestream_plugin_response.status_code == 409:
        print("Amazon Timestream plugin already enabled")
    else:
        raise Exception(f"Failed to enable Amazon Timestream plugin for workspace: {add_timestream_plugin_response.content}")

def add_timestream_data_source(service_account_token: str, grafana_workspace_url: str, grafana_data_source_name: str, region_name: str, database_name: str, table_name: str):
    """
    Adds a Timestream data source to Grafana.

    :param service_account_token: str: The Grafana workspace's service account token to use to make requests.
    :param grafana_workspace_url: str: The Grafana workspace's endpoint.
    :param grafana_data_source_name: str: The name to use for the data source.
    :param region_name: str: The AWS region that the Timestream for LiveAnalytics database and table are in.
    :param database_name: str: The Timestream for LiveAnalytics database that the workspace will use.
    :param table_name: str: The Timestream for LiveAnalytics table that the workspace will use.
    """
    headers = {
        "Authorization": f"Bearer {service_account_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    # Timestream data source payload
    data_source_payload = {
        "name": grafana_data_source_name,
        "type": "grafana-timestream-datasource",
        "access": "proxy",
        "jsonData": {
            "defaultRegion": region_name,
            "database": database_name,
            "table": table_name,
            "authenticationType": "AWS_IAM"
        }
    }

    # Post request to add the data source
    create_data_source_response = requests.post(
        f"https://{grafana_workspace_url}/api/datasources",
        headers=headers,
        data=json.dumps(data_source_payload)
    )

    if create_data_source_response.ok:
        print("Amazon Timestream for LiveAnalytics data source added successfully.")
    elif create_data_source_response.status_code == 409:  # Conflict - Data source already exists
        print("Amazon Timestream for LiveAnalytics data source already exists.")
    else:
        raise Exception(f"Failed to add Timestream data source: {create_data_source_response.content}")

def create_dashboard(service_account_token: str, grafana_workspace_url: str, dashboard_payload: dict) -> requests.Response:
    """
    Creates a dashboard in Grafana.

    :param service_account_token: str: The Grafana workspace's service account token to use to make requests.
    :param grafana_workspace_url: str: The Grafana workspace's endpoint.
    :param dashboard_payload: dict: The Grafana dashboard.
    :return: The requests.Response from creating the dashboard.
    """
    headers = {
        "Authorization": f"Bearer {service_account_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return requests.post(
        f"https://{grafana_workspace_url}/api/dashboards/db",
        headers=headers,
        data=json.dumps(dashboard_payload)
    )
