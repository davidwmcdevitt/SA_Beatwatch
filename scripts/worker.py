#!/usr/bin/env python3
import boto3
import pandas as pd
import re
import json
import datetime
import requests
import sys
import time
import os
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name="us-east-1")

bucket = "beatwatch"
hive_prefix = "inventory/sourceaudio-sad-archive/beatwatch-inventory/hive/"

# Transcribe queue URL (existing queue, not managed by this stack)
transcribe_queue_url = "https://sqs.us-east-1.amazonaws.com/158364657192/beatwatch-transcribe"

# Entry queue URL from environment
entry_queue_url = os.environ.get("ENTRY_QUEUE_URL")

# Slack webhook from environment
slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/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")

LOOKBACK_DAYS = 7

# Get instance ID for termination
instance_id = None
# First try environment variable (set by user-data script)
instance_id = os.environ.get("INSTANCE_ID")
if instance_id:
    instance_id = instance_id.strip()
    print(f"[{datetime.datetime.utcnow().isoformat()}] Successfully retrieved instance ID from environment: {instance_id}")
else:
    # Fall back to metadata service (with IMDSv2 support)
    try:
        # Get IMDSv2 token first
        token_response = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=2
        )
        if token_response.status_code == 200:
            token = token_response.text
            # Use token to get instance ID
            instance_id = requests.get(
                "http://169.254.169.254/latest/meta-data/instance-id",
                headers={"X-aws-ec2-metadata-token": token},
                timeout=2
            ).text.strip()
            print(f"[{datetime.datetime.utcnow().isoformat()}] Successfully retrieved instance ID from metadata: {instance_id}")
        else:
            print(f"[{datetime.datetime.utcnow().isoformat()}] WARNING: Failed to get IMDSv2 token: {token_response.status_code}")
    except Exception as e:
        print(f"[{datetime.datetime.utcnow().isoformat()}] WARNING: Failed to retrieve instance ID: {e}")
        pass


def log(message: str, level: str = "INFO"):
    """Helper function for consistent timestamped logging."""
    timestamp = datetime.datetime.utcnow().isoformat()
    print(f"[{timestamp}] [{level}] {message}")


def send_job_to_sqs(queue_url, input_s3_uri, output_s3_uri=None):
    """Send a transcription job to the transcribe queue."""
    log(f"Sending job to transcribe queue: input={input_s3_uri}, output={output_s3_uri}")
    try:
        body = {"input_s3_uri": input_s3_uri}
        if output_s3_uri:
            body["output_s3_uri"] = output_s3_uri

        resp = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(body),
            MessageAttributes={
                "JobType": {"StringValue": "beatwatch_transcribe", "DataType": "String"}
            },
        )
        log(f"Successfully sent job {resp['MessageId']} for {input_s3_uri}")
        return True
    except ClientError as e:
        log(f"Error sending SQS message: {e}", "ERROR")
        return False


def send_slack_message(message: str):
    """Send message to Slack."""
    if not slack_webhook_url:
        log("Slack webhook not configured, skipping Slack notification.")
        return

    payload = {
        "text": message,
        "channel": "#observation-deck",
        "username": "BeatwatchWorker",
        "icon_emoji": ":robot_face:"
    }

    try:
        log(f"Sending Slack message: {message[:100]}...")
        response = requests.post(slack_webhook_url, json=payload, timeout=10)
        if response.status_code != 200:
            log(f"Slack error: Status {response.status_code}, Response: {response.text}", "ERROR")
        else:
            log("Slack message sent successfully")
    except Exception as e:
        log(f"Failed to send Slack message: {e}", "ERROR")


def find_latest_hive_folder():
    """Find the latest Hive dt= folder in S3."""
    log(f"Searching for latest Hive folder in s3://{bucket}/{hive_prefix}")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=hive_prefix, Delimiter="/")
    folders = [p["Prefix"] for p in resp.get("CommonPrefixes", []) if p["Prefix"].startswith(hive_prefix + "dt=")]

    if not folders:
        log(f"No Hive dt= folders found in s3://{bucket}/{hive_prefix}", "ERROR")
        raise RuntimeError("No Hive dt= folders found")

    latest = sorted(folders)[-1]
    log(f"Found {len(folders)} Hive folders, using latest: {latest}")
    return latest


def find_channel_uris(latest_prefix, channel, lookback_days):
    """Find S3 URIs for files matching the specified channel and time window."""
    log(f"Reading symlink file from s3://{bucket}/{latest_prefix}symlink.txt")
    obj = s3.get_object(Bucket=bucket, Key=latest_prefix + "symlink.txt")
    symlinks = obj["Body"].read().decode().strip().splitlines()
    log(f"Found {len(symlinks)} symlinks in {latest_prefix}")

    pattern = re.compile(r'(' + channel + r')', re.IGNORECASE)
    results = []
    total_rows = 0
    now = pd.Timestamp.utcnow()
    day_ago = now - pd.Timedelta(days=lookback_days)

    log(f"Filtering criteria - Channel: {channel}, Time window: {day_ago} to {now} (lookback: {lookback_days} days)")

    for idx, s3_uri in enumerate(symlinks, 1):
        log(f"Processing symlink {idx}/{len(symlinks)}: {s3_uri}")
        try:
            for i, chunk in enumerate(pd.read_csv(s3_uri, compression="gzip", header=None, chunksize=500_000)):
                total_rows += len(chunk)
                if i == 0:
                    log(f"  Reading chunk 0 from {s3_uri} ({len(chunk)} rows)")

                chunk["channel"] = (
                    chunk[1]
                    .str.extract(pattern, expand=False)
                    .str.upper()
                )

                chunk["timestamp"] = pd.to_datetime(chunk[3], errors="coerce", utc=True)
                chunk["s3_uri"] = "s3://" + chunk[0] + "/" + chunk[1]

                time_mask = chunk["timestamp"].between(day_ago, now)
                channel_mask = chunk["channel"].isin([channel.upper()])

                filtered = chunk[time_mask & channel_mask][["s3_uri", "channel", "timestamp"]]

                if not filtered.empty:
                    log(f"  Found {len(filtered)} matching records in chunk {i} for channel {channel}")

                results += filtered.to_dict("records")

        except Exception as e:
            log(f"Error reading {s3_uri}: {e}", "ERROR")

    log(f"Completed scanning symlinks - Total rows scanned: {total_rows:,}, Total filtered results: {len(results)}")
    return results, total_rows


def process_channel(channel: str):
    """Process a single channel: find files and send to transcribe queue."""
    log(f"=== Starting processing for channel: {channel} ===")

    try:
        latest = find_latest_hive_folder()
        log(f"Using Hive folder: {latest}")
        
        recent_entries, total_inventory = find_channel_uris(latest, channel, LOOKBACK_DAYS)

        log(f"Channel {channel} inventory scan complete: {len(recent_entries)} matching items found from {total_inventory:,} total inventory rows")

        sent = 0
        skipped = 0

        if len(recent_entries) == 0:
            log(f"No entries found for channel {channel} in the last {LOOKBACK_DAYS} days")
        else:
            log(f"Processing {len(recent_entries)} entries for channel {channel}")

        for idx, entry in enumerate(recent_entries, 1):
            in_uri = entry["s3_uri"]
            timestamp = entry.get("timestamp", "unknown")
            filename = in_uri.split("/")[-1].replace(".ts", "")
            output_prefix = f"channels/{channel}/{filename}/"
            output_key = f"{output_prefix}transcription.json"

            log(f"  Entry {idx}/{len(recent_entries)}: {in_uri} (timestamp: {timestamp})")

            try:
                s3.head_object(Bucket=bucket, Key=output_key)
                log(f"  ✓ Transcription already exists: s3://{bucket}/{output_key} - SKIPPING")
                skipped += 1
                continue
            except ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    log(f"  ✗ Error checking existing transcription: {e}", "ERROR")
                    raise
                log(f"  → Transcription not found, will queue for processing")

            out_prefix = f"s3://{bucket}/channels/{channel}/"

            if send_job_to_sqs(transcribe_queue_url, in_uri, out_prefix):
                sent += 1
                log(f"  ✓ Successfully queued job {sent} for {in_uri}")
            else:
                log(f"  ✗ Failed to queue job for {in_uri}", "ERROR")

        log(f"=== Channel {channel} processing complete: sent={sent}, skipped={skipped} ===")
        return {"sent": sent, "skipped": skipped, "channel": channel}

    except Exception as e:
        log(f"ERROR processing channel {channel}: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return {"error": str(e), "channel": channel}




def main():
    """Main worker loop: poll queue, process channels, exit when done."""
    log("=" * 80)
    log("BeatwatchWorker starting initialization")
    log("=" * 80)
    
    if not entry_queue_url:
        log("ERROR: ENTRY_QUEUE_URL environment variable not set", "ERROR")
        sys.exit(1)

    log(f"Entry queue URL: {entry_queue_url}")
    log(f"Transcribe queue URL: {transcribe_queue_url}")
    log(f"Bucket: {bucket}")
    log(f"Lookback days: {LOOKBACK_DAYS}")
    log(f"Instance ID: {instance_id or 'UNKNOWN (not running on EC2)'}")
    log(f"BeatwatchWorker started at {datetime.datetime.utcnow().isoformat()}")

    channels_processed = []
    total_sent = 0
    total_skipped = 0
    poll_count = 0

    log("Starting queue polling loop...")
    while True:
        try:
            poll_count += 1
            log(f"Poll attempt #{poll_count}: Receiving message from queue (long polling, 20s wait)...")
            
            # Receive message from entry queue
            response = sqs.receive_message(
                QueueUrl=entry_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long polling
                VisibilityTimeout=3600,  # 1 hour visibility timeout
            )

            if "Messages" not in response or len(response["Messages"]) == 0:
                log("Queue is empty (no messages received). Exiting worker.")
                break

            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            message_id = message.get("MessageId", "unknown")
            
            log(f"Received message {message_id} from queue")
            log(f"Message body: {message.get('Body', 'N/A')[:200]}...")

            try:
                body = json.loads(message["Body"])
                channel = body.get("channel")

                if not channel:
                    log(f"Invalid message format (missing 'channel' field): {message}", "ERROR")
                    sqs.delete_message(QueueUrl=entry_queue_url, ReceiptHandle=receipt_handle)
                    log("Deleted invalid message from queue")
                    continue

                log(f"Processing channel from message: {channel}")

                # Process the channel
                result = process_channel(channel)

                if "error" not in result:
                    channels_processed.append(channel)
                    total_sent += result.get("sent", 0)
                    total_skipped += result.get("skipped", 0)
                    log(f"Channel {channel} processed successfully. Running totals: sent={total_sent}, skipped={total_skipped}")
                else:
                    log(f"Channel {channel} processing failed: {result.get('error')}", "ERROR")

                # Delete message from queue
                log(f"Deleting message {message_id} from queue")
                sqs.delete_message(QueueUrl=entry_queue_url, ReceiptHandle=receipt_handle)
                log(f"Message {message_id} deleted successfully")

            except json.JSONDecodeError as e:
                log(f"Error parsing message JSON: {e}", "ERROR")
                log(f"Message body was: {message.get('Body', 'N/A')}")
                sqs.delete_message(QueueUrl=entry_queue_url, ReceiptHandle=receipt_handle)
            except Exception as e:
                log(f"Error processing message: {e}", "ERROR")
                import traceback
                log(f"Traceback: {traceback.format_exc()}", "ERROR")
                # Delete message to avoid infinite retry (could move to DLQ instead)
                sqs.delete_message(QueueUrl=entry_queue_url, ReceiptHandle=receipt_handle)

        except KeyboardInterrupt:
            log("Interrupted by user (KeyboardInterrupt)")
            break
        except Exception as e:
            log(f"Error in main loop: {e}", "ERROR")
            import traceback
            log(f"Traceback: {traceback.format_exc()}", "ERROR")
            log("Waiting 10 seconds before retrying...")
            time.sleep(10)

    # Send summary and exit (Auto Scaling will handle instance termination when queue is empty)
    log("=" * 80)
    log("BeatwatchWorker finishing - generating summary")
    log("=" * 80)
    
    msg = (
        f"*BeatwatchWorker Stage 1 Summary*\n"
        f"Instance: {instance_id or 'unknown'}\n"
        f"Channels processed: {len(channels_processed)} ({', '.join(channels_processed) if channels_processed else 'none'})\n"
        f"Total files sent to transcribe queue: {total_sent}\n"
        f"Total files skipped (already processed): {total_skipped}\n"
    )
    
    log("Final summary:")
    log(f"  Instance ID: {instance_id or 'unknown'}")
    log(f"  Channels processed: {len(channels_processed)}")
    log(f"  Channels list: {', '.join(channels_processed) if channels_processed else '(none)'}")
    log(f"  Total files sent to transcribe queue: {total_sent}")
    log(f"  Total files skipped (already processed): {total_skipped}")
    log(f"  Total poll attempts: {poll_count}")
    
    send_slack_message(msg)
    log("Summary sent to Slack")
    print("\n" + msg)

    # Exit with code 0 for success - Auto Scaling will scale down and terminate the instance
    log("Exiting with code 0 (success)")
    sys.exit(0)


if __name__ == "__main__":
    import os
    main()

