import boto3
import json
import os
import requests
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name="us-east-1")

bucket = "beatwatch"
hive_prefix = "inventory/sourceaudio-sad-archive/beatwatch-inventory/hive/"

# Channels to monitor - from environment variable or default
CHANNELS_STR = os.environ.get("CHANNELS", "TNT,NBATV,MTV2HD")
CHANNELS = set([ch.strip().upper() for ch in CHANNELS_STR.split(",")])

# Queue URL from environment
entry_queue_url = os.environ.get("ENTRY_QUEUE_URL")

# Slack webhook from environment
slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/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")


def send_slack_message(message: str):
    """Send message to Slack."""
    if not slack_webhook_url:
        print("Slack webhook not configured.")
        return

    payload = {
        "text": message,
        "channel": "#observation-deck",
        "username": "BeatwatchSend",
        "icon_emoji": ":robot_face:"
    }

    try:
        response = requests.post(slack_webhook_url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Slack error: Status {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Failed to send Slack message: {e}")


def find_latest_hive_folder():
    """Find the latest Hive dt= folder in S3."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=hive_prefix, Delimiter="/")
    folders = [p["Prefix"] for p in resp.get("CommonPrefixes", []) if p["Prefix"].startswith(hive_prefix + "dt=")]

    if not folders:
        raise RuntimeError("No Hive dt= folders found")

    latest = sorted(folders)[-1]
    print(f"Latest Hive folder: {latest}")
    return latest


def send_channel_to_queue(channel: str):
    """Send a channel ID to the entry queue."""
    try:
        body = {"channel": channel}
        resp = sqs.send_message(
            QueueUrl=entry_queue_url,
            MessageBody=json.dumps(body),
            MessageAttributes={
                "JobType": {"StringValue": "beatwatch_channel", "DataType": "String"}
            },
        )
        print(f"Sent channel {channel} to queue: {resp['MessageId']}")
        return True
    except ClientError as e:
        print(f"Error sending SQS message for channel {channel}: {e}")
        return False


def lambda_handler(event, context=None):
    """Main Lambda handler - sends channel IDs to the entry queue."""
    import datetime
    version_time = datetime.datetime.utcnow().isoformat()
    print(f"BeatwatchSend Lambda launched at {version_time}")

    if not entry_queue_url:
        error_msg = "ENTRY_QUEUE_URL environment variable not set"
        print(f"ERROR: {error_msg}")
        send_slack_message(f"BeatwatchSend failed: {error_msg}")
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

    try:
        # Find latest hive folder to verify it exists
        latest = find_latest_hive_folder()

        # Send each channel to the queue
        sent = 0
        failed = 0
        for channel in sorted(CHANNELS):
            if send_channel_to_queue(channel):
                sent += 1
            else:
                failed += 1

        msg = (
            f"*BeatwatchSend Summary*\n"
            f"Channels in target list: {len(CHANNELS)} ({', '.join(sorted(CHANNELS))})\n"
            f"Channels successfully sent to queue: {sent}\n"
            f"Channels failed to send: {failed}\n"
            f"Latest Hive folder: {latest}"
        )

        send_slack_message(msg)
        print(msg)

        return {"statusCode": 200, "body": json.dumps({"message": msg, "sent": sent, "failed": failed})}

    except Exception as e:
        err = f"BeatwatchSend failed: {e}"
        print(err)
        send_slack_message(err)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

