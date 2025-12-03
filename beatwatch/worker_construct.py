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

        # Allow reading from S3 inventory bucket
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    "arn:aws:s3:::beatwatch",
                    "arn:aws:s3:::beatwatch/*",
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

        # Allow lifecycle hook operations (for potential future handler scripts)
        worker_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:DescribeInstances",
                    "ec2:CreateTags",
                    "autoscaling:CompleteLifecycleAction",
                    "autoscaling:RecordLifecycleActionHeartbeat",
                    "autoscaling:DescribeAutoScalingInstances",
                    "autoscaling:DescribeLifecycleHooks",
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

        # User data script with proper logging and error handling
        user_data = ec2.UserData.for_linux()

        user_data.add_commands(
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
            # Worker environment
            f"export ENTRY_QUEUE_URL={queue.queue_url}",
            "export AWS_DEFAULT_REGION=us-east-1",
            # Instance metadata for logging
            "export INSTANCE_ID=$(ec2-metadata --instance-id | cut -d ' ' -f 2)",
            "export INSTANCE_TYPE=$(ec2-metadata --instance-type | cut -d ' ' -f 2)",
            # Exit handler - log exit code for debugging (Auto Scaling will handle termination)
            "exit_handler() {",
            "    EXIT_CODE=$?",
            "    echo \"Worker script exited with code $EXIT_CODE\"",
            "}",
            "trap exit_handler EXIT",
            # Install Python dependencies (after trap is set up)
            "python3.11 -m pip install --upgrade pip",
            "python3.11 -m pip install -r requirements.txt",
            "chmod +x worker.py",
            # Run worker
            "python3.11 worker.py",
        )

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
            default_result=autoscaling.DefaultResult.CONTINUE,
            heartbeat_timeout=Duration.seconds(300),
        )

        # Scale based on queue depth
        self.asg.scale_on_metric(
            "ScaleOnQueueDepth",
            metric=queue.metric_approximate_number_of_messages_visible(),
            scaling_steps=[
                autoscaling.ScalingInterval(lower=0, upper=10, change=1),   # 0-10  -> 1 instance
                autoscaling.ScalingInterval(lower=10, upper=30, change=2),  # 10-30 -> 2 instances (overlaps at 10)
                autoscaling.ScalingInterval(lower=30, upper=50, change=3),  # 30-50 -> 3 instances (overlaps at 30)
                autoscaling.ScalingInterval(lower=50, change=5),            # 50+   -> 5 instances (overlaps at 50)
            ],
            adjustment_type=autoscaling.AdjustmentType.EXACT_CAPACITY,
        )
