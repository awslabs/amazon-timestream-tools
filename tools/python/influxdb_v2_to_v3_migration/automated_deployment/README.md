# Amazon Timestream for InfluxDB v2 to v3 Migration Script Automated Deployment

## Overview

The automated deployment of the Timestream for InfluxDB v2 to v3 migration script deploys an EC2 instance with all required dependencies installed. Specifically, the EC2 instance will have:
- Python 3.13 installed and added to the default user's PATH as `python`.
- All Python dependencies installed globally.
- `influxdb_v2_to_v3_migration.py` and `influxdb_v3_ingestion.py` in the default user's home directory.
- [AWS Systems Manager (AWS SSM)](https://aws.amazon.com/systems-manager/) access enabled.

Automated deployment is split into two stages:
1. AMI creation using [Packer](https://developer.hashicorp.com/packer). Creating an AMI means that the deployed EC2 instance can be within a private VPC, no internet access is required.
2. Resource deployment using [Terraform](https://developer.hashicorp.com/terraform). Terraform will deploy the following resources:
   - An [`aws_security_group`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group).
   - An [`aws_secretsmanager_secret`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/secretsmanager_secret).
   - An [`aws_secretsmanager_secret_version`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/secretsmanager_secret_version).
   - An [`aws_iam_role`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role).
   - An [`aws_iam_policy`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy).
   - Two [`aws_iam_role_policy_attachment`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) resources.
   - An [`aws_iam_instance_profile`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_instance_profile).
   - An [`aws_instance`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/instance).

## Permissions

To use the migration script and deploy all resources, you must have the following IAM permissions, replacing `<region>` with the AWS region that resources will be deployed in and `<account ID>` with your AWS account ID:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EC2Core",
            "Effect": "Allow",
            "Action": [
                "ec2:RunInstances",
                "ec2:TerminateInstances",
                "ec2:StartInstances",
                "ec2:StopInstances",
                "ec2:ModifyInstanceAttribute",
                "ec2:CreateImage",
                "ec2:DeregisterImage",
                "ec2:CreateSnapshot",
                "ec2:DeleteSnapshot",
                "ec2:CreateVolume",
                "ec2:DeleteVolume",
                "ec2:AttachVolume",
                "ec2:DetachVolume",
                "ec2:CreateSecurityGroup",
                "ec2:DeleteSecurityGroup",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:AuthorizeSecurityGroupEgress",
                "ec2:RevokeSecurityGroupIngress",
                "ec2:RevokeSecurityGroupEgress",
                "ec2:CreateKeyPair",
                "ec2:DeleteKeyPair",
                "ec2:CreateTags",
                "ec2:DeleteTags",
                "ec2:DescribeTags",
                "ec2:DescribeInstances",
                "ec2:DescribeImages",
                "ec2:DescribeSnapshots",
                "ec2:DescribeVolumes",
                "ec2:DescribeVpcs",
                "ec2:DescribeVpcAttribute",
                "ec2:DescribeInstanceAttribute",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeRegions",
                "ec2:DescribeInstanceTypes",
                "ec2:DescribeInstanceCreditSpecifications",
                "ec2:DescribeNetworkInterfaces"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "<region>"
                }
            }
        },
        {
            "Sid": "IAMResources",
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:DeleteRole",
                "iam:GetRole",
                "iam:CreatePolicy",
                "iam:DeletePolicy",
                "iam:GetPolicy",
                "iam:GetPolicyVersion",
                "iam:AttachRolePolicy",
                "iam:DetachRolePolicy",
                "iam:PutRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:CreateInstanceProfile",
                "iam:DeleteInstanceProfile",
                "iam:AddRoleToInstanceProfile",
                "iam:RemoveRoleFromInstanceProfile",
                "iam:GetInstanceProfile",
                "iam:ListRolePolicies",
                "iam:ListAttachedRolePolicies",
                "iam:ListInstanceProfilesForRole",
                "iam:ListPolicyVersions"
            ],
            "Resource": [
                "arn:aws:iam::<account ID>:role/*influxdb_v2_to_v3_migration_runner_role*",
                "arn:aws:iam::<account ID>:policy/*influxdb_v2_to_v3_migration_runner_policy*",
                "arn:aws:iam::<account ID>:instance-profile/*influxdb_v2_to_v3_migration_runner_profile*"
            ]
        },
        {
            "Sid": "PassRoleToEC2",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::<account ID>:role/*influxdb_v2_to_v3_migration_runner_role*",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "ec2.amazonaws.com"
                }
            }
        },
        {
            "Sid": "SecretsManager",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:CreateSecret",
                "secretsmanager:DeleteSecret",
                "secretsmanager:PutSecretValue",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:GetResourcePolicy"
            ],
            "Resource": "arn:aws:secretsmanager:<region>:<account ID>:secret:*influxdb_v2_to_v3_migration*"
        },
        {
            "Sid": "SSMParameters",
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter",
                "ssm:GetParameters",
                "ssm:PutParameter"
            ],
            "Resource": "arn:aws:ssm:<region>:<account ID>:parameter/amis/influxdb-v2-to-v3-migration-runner/*"
        },
        {
            "Sid": "SSMSession",
            "Effect": "Allow",
            "Action": "ssm:StartSession",
            "Resource": [
                "arn:aws:ssm:<region>:<account ID>:document/SSM-SessionManagerRunShell",
                "arn:aws:ec2:<region>:<account ID>:instance/*"
            ]
        }
    ]
}
```

## Steps

1. [Download and install Packer](https://developer.hashicorp.com/packer/install). Packer will be used to create an AMI with all necessary dependencies and scripts. This AMI will be used later to deploy an EC2 instance.
2. [Download and install Terraform](https://developer.hashicorp.com/terraform/install). Terraform will be used to deploy an EC2 instance and all other necessary resources for the EC2 instance to perform a migration.
3. [Create an EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/create-key-pairs.html) to use to SSH onto your deployed EC2 instance if you don't already have an existing EC2 key pair.
4. Update [`variables.tf`](./variables.tf), filling in all `"replace me"` placeholders:
   
   - `vpc_id`: The ID of an existing VPC.
   - `subnet_id`: The ID of an existing subnet in the above VPC.
   - `ssh_access_ip`: The IP to grant SSH access to the deployed EC2 instance. For example, `127.0.0.1/32`.
   - `tokens`: Your InfluxDB v2 and v3 tokens. These tokens will be placed in a secret in AWS Secrets Manager and redacted from all Terraform output.
   - `runner_ssh_key_name`: The name of an existing EC2 key pair you wish to use to SSH onto your deployed EC2 instance.
5. Initialize Packer and build the AMI, this will produce an AMI in your account with the name `influxdb-v2-to-v3-migration-runner-<timestamp>`. This can take approximately 5 to 8 minutes:
   ```shell
   packer init packer.pkr.hcl
   packer build packer.pkr.hcl
   ```
6. Initialize and apply Terraform changes:
   ```shell
   terraform init
   terraform apply
   ```
7. Review the proposed changes by Terraform and type `yes`.

8. Take note of the output `runner_ip` value. This IP will need to be added as an ingress rule to your Timestream for InfluxDB v2 and v3 security groups. This can be accomplished with the AWS CLI:
   ```shell
   # InfluxDB v2.
   aws ec2 authorize-security-group-ingress \
       --group-id <InfluxDB v2 security group ID> \
       --protocol tcp \
       --port 8086 \
       --cidr <runner_ip>/32

   # InfluxDB v3.
   aws ec2 authorize-security-group-ingress \
       --group-id <InfluxDB v3 security group ID> \
       --protocol tcp \
       --port 8181 \
       --cidr <runner_ip>/32
   ```
9. Using the key pair you specified in `variables.tf`, SSH onto the instance, using the output `runner_ip`:
   ```shell
   ssh -i <path to private key> ec2-user@<runner_ip>
   ```
   - If you don't want to use SSH, you can use AWS SSM instead:
      ```shell
      aws ssm start-session --target <instance ID>
      ```
      Once you have started an SSM session, switch to the `ec2-user` user:
      ```shell
      sudo su - ec2-user
      ```

10. Run the script, providing:
    - Your InfluxDB v2 URL.
    - Your InfluxDB v3 URL.
    - Either:
      - The InfluxDB v2 buckets that you want to migrate and their organizations, with `--source-buckets-and-orgs`.
      - Or, the names of the organizations to migrate all buckets from, with `--source-orgs`.
    - The name of the secret you created in AWS Secrets Manager that contains your InfluxDB v2 and v3 tokens.
    ```shell
    python influxdb_v2_to_v3_migration.py \
        --source-url "https://example.com:8086" \
        --destination-url "https://example.com:8181" \
        --source-buckets-and-orgs "bucket-one:organization-one,bucket-two:organization-two" \
        --tokens-secret-name "influxdb_v2_to_v3_migration"
    ```
    - **Note**: Packer installs Python 3.13 in the AMI simply as `python`.

## Clean Up

1. Destroy all Terraform-deployed resources:
   ```shell
   terraform destroy
   ```
2. When prompted, type `yes`.
3. Find, [disable](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disable-an-ami.html), and [deregister](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/deregister-ami.html) all `influxdb-v2-to-v3-migration-runner-<timestamp>` AMIs.
4. In AWS Systems Manager, delete the `/amis/influxdb-v2-to-v3-migration-runner/latest` parameter in the parameter store:
   ```shell
   aws ssm delete-paramter \
       --name "/amis/influxdb-v2-to-v3-migration-runner/latest"
   ```
