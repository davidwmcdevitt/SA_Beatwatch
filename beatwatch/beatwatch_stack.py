from aws_cdk import (
    Duration,
    Stack,
    Tags,
    BundlingOptions,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_cloudwatch as cloudwatch,
    aws_logs as logs,
    CfnOutput,
)
from constructs import Construct
from .worker_construct import BeatwatchWorker
import os
import json


class BeatwatchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # === 1. VPC ===
        vpc = ec2.Vpc.from_lookup(self, "MainVPC", vpc_id="vpc-915ffff4")

        # === 2. ENTRY QUEUE ===
        entry_dlq = sqs.Queue(
            self, "BeatwatchEntryDLQ",
            queue_name="BeatwatchEntry-DLQ",
        )

        entry_queue = sqs.Queue(
            self, "BeatwatchEntryQueue",
            queue_name="BeatwatchEntry",
            visibility_timeout=Duration.seconds(3600),  # 1 hour
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=entry_dlq,
            ),
        )


        # Load webhook from config file or environment variable
        def load_webhook_from_config():
            """Load webhook URL from config.json file."""
            try:
                # Get the project root directory (parent of beatwatch/)
                current_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(current_dir)
                config_path = os.path.join(project_root, "config.json")
                with open(config_path, "r") as f:
                    config = json.load(f)
                    return config.get("slack_webhook_url")
            except (FileNotFoundError, json.JSONDecodeError, KeyError):
                return None

        slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL") or load_webhook_from_config()

        # === 4. LAMBDA FUNCTION ===
        lambda_fn = _lambda.Function(
            self, "BeatwatchDailySender",
            function_name="BeatwatchDailySender",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="daily_sender.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                ),
            ),
            timeout=Duration.seconds(600),
            log_retention=logs.RetentionDays.TWO_YEARS,
            environment={
                "ENTRY_QUEUE_URL": entry_queue.queue_url,
                "CHANNELS": "TNT,NBATV,MTV2HD",
                "SLACK_WEBHOOK_URL": slack_webhook_url or "",
            },
        )

        # Lambda IAM permissions
        # Allow reading from S3 inventory bucket
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    "arn:aws:s3:::beatwatch",
                    "arn:aws:s3:::beatwatch/*",
                ],
            )
        )

        # Grant send message permission to entry queue
        entry_queue.grant_send_messages(lambda_fn)

        # === 5. EVENTBRIDGE SCHEDULE (7am Chicago time) ===
        # Chicago is UTC-6 (CST) or UTC-5 (CDT)
        # 7am CST = 13:00 UTC, 7am CDT = 12:00 UTC
        # Using 13:00 UTC as default (CST)
        schedule = events.Schedule.cron(
            hour="13",
            minute="0",
        )

        events.Rule(
            self, "BeatwatchDailySchedule",
            description="Trigger Beatwatch daily sender at 7am Chicago time",
            schedule=schedule,
            targets=[targets.LambdaFunction(lambda_fn)],
        )

        # === 6. WORKER CONSTRUCT ===
        # ARN for existing beatwatch-transcribe queue
        transcribe_queue_arn = f"arn:aws:sqs:us-east-1:158364657192:beatwatch-transcribe"

        worker = BeatwatchWorker(
            self, "BeatwatchWorker",
            queue=entry_queue,
            vpc=vpc,
            transcribe_queue_arn=transcribe_queue_arn,
        )

        # === 7. CLOUDWATCH ALARMS ===
        # Entry queue DLQ message count alarm
        entry_dlq_alarm = cloudwatch.Alarm(
            self, "EntryDLQAlarm",
            metric=entry_dlq.metric_approximate_number_of_messages_visible(),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Alert when messages appear in entry queue DLQ",
        )

        # Entry queue age of oldest message alarm
        entry_queue_age_alarm = cloudwatch.Alarm(
            self, "EntryQueueAgeAlarm",
            metric=entry_queue.metric_approximate_age_of_oldest_message(),
            threshold=3600,  # 1 hour in seconds
            evaluation_periods=2,
            alarm_description="Alert when messages in entry queue are older than 1 hour",
        )

        # Lambda function errors alarm
        lambda_errors_alarm = cloudwatch.Alarm(
            self, "LambdaErrorsAlarm",
            metric=lambda_fn.metric_errors(),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Alert when Lambda function has errors",
        )

        # === 8. STACK TAGS ===
        Tags.of(self).add("Project", "Beatwatch")

        # === 9. OUTPUTS ===
        CfnOutput(self, "EntryQueueUrl", value=entry_queue.queue_url)
        CfnOutput(self, "EntryQueueArn", value=entry_queue.queue_arn)
        CfnOutput(self, "LambdaFunctionName", value=lambda_fn.function_name)
        CfnOutput(self, "WorkerASGName", value=worker.asg.auto_scaling_group_name)
