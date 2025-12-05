from aws_cdk import (
    Duration,
    Tags,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_autoscaling as autoscaling,
    aws_sqs as sqs,
    aws_s3_assets as s3_assets,
)
from constructs import Construct
import os
import json


class BeatwatchWorker(Construct):
    """Construct that creates an Auto Scaling Group of EC2 instances that process channel IDs from SQS."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        queue: sqs.Queue,
        vpc: ec2.IVpc,
        transcribe_queue_arn: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # IAM role for EC2 instances
        worker_role = iam.Role(
            self, "WorkerRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchAgentServerPolicy"),
            ],
        )

        # Allow reading and writing to beatwatch bucket
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:HeadObject"],
                resources=[
                    "arn:aws:s3:::beatwatch",
                    "arn:aws:s3:::beatwatch/*",
                ],
            )
        )

        # Allow reading, writing, and deleting from batch processor bucket
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:HeadObject", "s3:DeleteObject"],
                resources=[
                    "arn:aws:s3:::sourceaudio-gpt-batch-processor",
                    "arn:aws:s3:::sourceaudio-gpt-batch-processor/*",
                ],
            )
        )

        # Grant SQS consume messages for entry queue
        queue.grant_consume_messages(worker_role)

        # Allow sending messages to transcribe queue (existing queue)
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[transcribe_queue_arn],
            )
        )

        # Allow lifecycle hook operations and instance termination
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "autoscaling:CompleteLifecycleAction",
                    "autoscaling:RecordLifecycleActionHeartbeat",
                    "autoscaling:DescribeAutoScalingInstances",
                    "autoscaling:DescribeLifecycleHooks",
                    "ec2:TerminateInstances",
                    "ec2:DescribeInstances",
                ],
                resources=["*"],
            )
        )

        # S3 asset for worker script
        script_asset = s3_assets.Asset(
            self,
            "BeatwatchWorkerScript",
            path=os.path.join(os.path.dirname(__file__), "..", "scripts", "worker.py"),
        )
        script_asset.grant_read(worker_role)

        # S3 asset for requirements file
        requirements_asset = s3_assets.Asset(
            self,
            "BeatwatchWorkerRequirements",
            path=os.path.join(os.path.dirname(__file__), "..", "scripts", "requirements.txt"),
        )
        requirements_asset.grant_read(worker_role)

        # Load Pushover credentials from config.json
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
        pushover_app_token = None
        pushover_user_key = None
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                pushover_app_token = config.get("pushover_app_token")
                pushover_user_key = config.get("pushover_user_key")
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Could not load Pushover credentials from config.json: {e}")

        # User data script with proper logging and error handling
        user_data = ec2.UserData.for_linux()

        # Build user data commands
        user_data_commands = [
            "#!/bin/bash",
            "set -euxo pipefail",
            # Logging setup
            "exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1",
            # Install dependencies
            "sudo dnf update -y",
            "sudo dnf install -y python3.11 python3.11-pip aws-cli ec2-utils",
            # Worker directory
            "mkdir -p /home/ec2-user/beatwatch-worker",
            "cd /home/ec2-user/beatwatch-worker",
            # Pull script and requirements from S3
            f"aws s3 cp s3://{script_asset.s3_bucket_name}/{script_asset.s3_object_key} ./worker.py",
            f"aws s3 cp s3://{requirements_asset.s3_bucket_name}/{requirements_asset.s3_object_key} ./requirements.txt",
            # Install Python dependencies
            "python3.11 -m pip install --upgrade pip",
            "python3.11 -m pip install -r requirements.txt",
            # Worker environment
            f"export ENTRY_QUEUE_URL={queue.queue_url}",
            "export AWS_DEFAULT_REGION=us-east-1",
        ]
        
        # Add Pushover credentials if configured
        if pushover_app_token:
            user_data_commands.append(f"export PUSHOVER_APP_TOKEN={pushover_app_token}")
        if pushover_user_key:
            user_data_commands.append(f"export PUSHOVER_USER_KEY={pushover_user_key}")
        
        # Add remaining commands
        user_data_commands.extend([
            # Instance metadata for logging
            "export INSTANCE_ID=$(ec2-metadata --instance-id | cut -d ' ' -f 2)",
            "export INSTANCE_TYPE=$(ec2-metadata --instance-type | cut -d ' ' -f 2)",
            # ASG metadata for lifecycle hooks
            "ASG_NAME=$(aws autoscaling describe-auto-scaling-instances --instance-ids $INSTANCE_ID "
            "--query 'AutoScalingInstances[0].AutoScalingGroupName' --output text)",
            "export ASG_NAME",
            # Make script executable
            "chmod +x worker.py",
            # Run worker (termination handled within Python script via atexit)
            "python3.11 worker.py",
        ])
        
        user_data.add_commands(*user_data_commands)

        # Launch template
        launch_template = ec2.LaunchTemplate(
            self, "LaunchTemplate",
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            role=worker_role,
            user_data=user_data,
            require_imdsv2=True,
        )

        # Add tags to launch template
        Tags.of(launch_template).add("Name", "Beatwatch-Worker")
        Tags.of(launch_template).add("WorkerType", "Beatwatch")
        Tags.of(launch_template).add("Project", "Beatwatch")

        # Auto Scaling Group
        self.asg = autoscaling.AutoScalingGroup(
            self, "AutoScalingGroup",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            launch_template=launch_template,
            min_capacity=0,
            max_capacity=5,
            desired_capacity=0,
            cooldown=Duration.seconds(300),
        )

        # Add tags to ASG
        Tags.of(self.asg).add("Name", "BeatwatchWorker")
        Tags.of(self.asg).add("Project", "Beatwatch")

        # Add lifecycle hook for graceful termination
        self.asg.add_lifecycle_hook(
            "BeatwatchWorkerTerminationHook",
            lifecycle_transition=autoscaling.LifecycleTransition.INSTANCE_TERMINATING,
            default_result=autoscaling.DefaultResult.ABANDON,
            heartbeat_timeout=Duration.seconds(300),
        )

        # Scale based on queue depth - matches vocal annotation scaling intervals
        self.asg.scale_on_metric(
            "ScaleOnQueueDepth",
            metric=queue.metric_approximate_number_of_messages_visible(),
            scaling_steps=[
                autoscaling.ScalingInterval(upper=1, change=0),           # 0 msgs    -> 0
                autoscaling.ScalingInterval(lower=1, upper=11, change=1),   # 1-10    -> 1
                autoscaling.ScalingInterval(lower=11, upper=31, change=2),  # 11-30   -> 2
                autoscaling.ScalingInterval(lower=31, upper=51, change=3),  # 31-50   -> 3
                autoscaling.ScalingInterval(lower=51, change=5),            # 51+     -> 5
            ],
            adjustment_type=autoscaling.AdjustmentType.EXACT_CAPACITY,
        )
