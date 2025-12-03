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
ec2 = boto3.client("ec2", region_name="us-east-1")
autoscaling = boto3.client("autoscaling", region_name="us-east-1")

bucket = "beatwatch"
hive_prefix = "inventory/sourceaudio-sad-archive/beatwatch-inventory/hive/"

# Transcribe queue URL (existing queue, not managed by this stack)
transcribe_queue_url = "https://sqs.us-east-1.amazonaws.com/158364657192/beatwatch-transcribe"

# Entry queue URL from environment
entry_queue_url = os.environ.get("ENTRY_QUEUE_URL")

# Slack webhook from config file or environment variable
def load_webhook_from_config():
    """Load webhook URL from config.json file."""
    try:
        # Get the project root directory (parent of scripts/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        config_path = os.path.join(project_root, "config.json")
        with open(config_path, "r") as f:
            config = json.load(f)
            return config.get("slack_webhook_url")
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None

slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL") or load_webhook_from_config()

LOOKBACK_DAYS = 7

# Get instance metadata
instance_id = os.environ.get("INSTANCE_ID", "").strip()
asg_name = os.environ.get("ASG_NAME", "").strip()


def log(message: str, level: str = "INFO"):
    """Helper function for consistent timestamped logging."""
    timestamp = datetime.datetime.utcnow().isoformat()
    print(f"[{timestamp}] [{level}] {message}")


def terminate_instance(exit_code: int):
    """
    Terminate this instance following crash-and-terminate pattern.
    Called via atexit, ensures instance cleanup regardless of exit reason.
    """
    log(f"Worker script exited with code {exit_code}")
    
    if not instance_id or instance_id == "unknown":
        log("Instance ID not available, skipping termination", "WARNING")
        return
    
    # Determine lifecycle action result
    if exit_code == 0:
        log("Success - completing lifecycle hook and terminating")
        lifecycle_result = "CONTINUE"
    else:
        log(f"Error exit code {exit_code} - completing lifecycle hook and terminating")
        lifecycle_result = "ABANDON"
    
    # Complete lifecycle hook if exists
    if asg_name:
        try:
            log(f"Completing lifecycle hook for ASG: {asg_name}")
            autoscaling.complete_lifecycle_action(
                LifecycleActionResult=lifecycle_result,
                LifecycleHookName="BeatwatchWorkerTerminationHook",
                AutoScalingGroupName=asg_name,
                InstanceId=instance_id
            )
            log("Lifecycle hook completed successfully")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ValidationError":
                log("No lifecycle hook to complete (not in terminating state)", "INFO")
            else:
                log(f"Failed to complete lifecycle hook: {e}", "WARNING")
        except Exception as e:
            log(f"Failed to complete lifecycle hook: {e}", "WARNING")
    
    # Always terminate instance
    try:
        log(f"Terminating instance: {instance_id}")
        ec2.terminate_instances(InstanceIds=[instance_id])
        log("Instance termination initiated successfully")
    except Exception as e:
        log(f"Failed to terminate instance: {e}", "ERROR")


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
    log(f"Instance ID: {instance_id or 'unknown'}")
    log(f"ASG Name: {asg_name or 'unknown'}")
    log(f"BeatwatchWorker started at {datetime.datetime.utcnow().isoformat()}")

    channels_processed = []
    total_sent = 0
    total_skipped = 0
    poll_count = 0
    empty_polls = 0
    max_empty_polls = 5  # Exit after 5 empty polls (~75 seconds with WaitTimeSeconds=15)
    log(f"Starting queue polling loop (max {max_empty_polls} empty polls before exit)...")
    
    while True:
        try:
            poll_count += 1
            log(f"Poll attempt #{poll_count}: Receiving message from queue (long polling, 15s wait)...")
            
            # Receive message from entry queue
            response = sqs.receive_message(
                QueueUrl=entry_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=15,  # Long polling
                VisibilityTimeout=3600,  # 1 hour visibility timeout
            )

            if "Messages" not in response or len(response["Messages"]) == 0:
                empty_polls += 1
                log(f"No messages (empty poll {empty_polls}/{max_empty_polls})")
                
                if empty_polls >= max_empty_polls:
                    log(f"Reached {max_empty_polls} empty polls, exiting gracefully")
                    break
                
                time.sleep(3)
                continue

            # Reset empty poll counter when we get a message
            empty_polls = 0

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

    # Send summary and exit
    log("=" * 80)
    log("BeatwatchWorker finishing - generating summary")
    log("=" * 80)
    
    msg = (
        f"*BeatwatchWorker Summary*\n"
        f"Instance: {instance_id or 'unknown'}\n"
        f"Channels processed: {len(channels_processed)} ({', '.join(channels_processed) if channels_processed else 'none'})\n"
        f"Total files sent to transcribe queue: {total_sent}\n"
        f"Total files skipped (already processed): {total_skipped}\n"
    )
    
    log("Final summary:")
    log(f"  Instance ID: {instance_id or 'unknown'}")
    log(f"  ASG Name: {asg_name or 'unknown'}")
    log(f"  Channels processed: {len(channels_processed)}")
    log(f"  Channels list: {', '.join(channels_processed) if channels_processed else '(none)'}")
    log(f"  Total files sent to transcribe queue: {total_sent}")
    log(f"  Total files skipped (already processed): {total_skipped}")
    log(f"  Total poll attempts: {poll_count}")
    
    send_slack_message(msg)
    log("Summary sent to Slack")
    print("\n" + msg)
    log("Exiting with code 0 (success)")
    sys.exit(0)


if __name__ == "__main__":
    import atexit
    
    # Track exit code for termination handler (use list for closure access)
    _exit_code = [0]
    
    # Register termination handler
    def exit_handler():
        terminate_instance(_exit_code[0])
    
    atexit.register(exit_handler)
    
    try:
        main()
    except Exception as e:
        _exit_code[0] = 1
        log(f"Uncaught exception in main: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        sys.exit(1)

