import boto3
import json
import os
import requests
import datetime
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

# Pushover credentials from environment variables (set by CDK from config.json)
pushover_app_token = os.environ.get("PUSHOVER_APP_TOKEN")
pushover_user_key = os.environ.get("PUSHOVER_USER_KEY")


def save_failed_pushover_message_to_s3(message: str, error_info: dict):
    """Save failed Pushover message to S3 for later review."""
    try:
        timestamp = datetime.datetime.utcnow().isoformat().replace(":", "-")
        key = f"failed-pushover-messages/{timestamp}.json"
        
        data = {
            "original_message": message,
            "error_info": error_info,
            "timestamp": timestamp,
            "source": "BeatwatchSend"
        }
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        print(f"Saved failed Pushover message to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to save message to S3: {e}")
        return False


def send_pushover_message(message: str):
    """Send message to Pushover."""
    if not pushover_app_token or not pushover_user_key:
        print("Pushover credentials not configured.")
        return

    try:
        print(f"Sending Pushover message: {message[:100]}...")
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": pushover_app_token,
                "user": pushover_user_key,
                "message": message
            },
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") != 1:
            # Enhanced structured logging with message content
            error_info = {
                "status_code": response.status_code,
                "response_data": data,
                "message_preview": message[:500],  # First 500 chars
                "message_length": len(message),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            error_data = {
                "event": "pushover_api_failure",
                **error_info
            }
            print(f"ERROR: Pushover error: {json.dumps(error_data)}")
            # Save to S3 for recovery
            save_failed_pushover_message_to_s3(message, error_info)
    except requests.RequestException as e:
        # Enhanced structured logging with message content
        error_info = {
            "exception_type": type(e).__name__,
            "exception_message": str(e),
            "message_preview": message[:500],
            "message_length": len(message),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        error_data = {
            "event": "pushover_api_exception",
            **error_info
        }
        print(f"ERROR: Failed to send Pushover message: {json.dumps(error_data)}")
        # Save to S3 for recovery
        save_failed_pushover_message_to_s3(message, error_info)


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
        send_pushover_message(f"BeatwatchSend failed: {error_msg}")
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

        send_pushover_message(msg)
        print(msg)

        return {"statusCode": 200, "body": json.dumps({"message": msg, "sent": sent, "failed": failed})}

    except Exception as e:
        err = f"BeatwatchSend failed: {e}"
        print(err)
        send_pushover_message(err)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

