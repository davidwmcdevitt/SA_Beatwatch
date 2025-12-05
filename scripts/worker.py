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
import tempfile
import shutil
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

# Pushover credentials from config file or environment variables
def load_pushover_from_config():
    """Load Pushover credentials from config.json file."""
    try:
        # Get the project root directory (parent of scripts/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        config_path = os.path.join(project_root, "config.json")
        with open(config_path, "r") as f:
            config = json.load(f)
            return config.get("pushover_app_token"), config.get("pushover_user_key")
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None, None

config_token, config_user_key = load_pushover_from_config()
pushover_app_token = os.environ.get("PUSHOVER_APP_TOKEN") or config_token
pushover_user_key = os.environ.get("PUSHOVER_USER_KEY") or config_user_key

LOOKBACK_DAYS = 7

# Batch processing constants
BATCH_INPUT_BUCKET = "sourceaudio-gpt-batch-processor"
BATCH_INPUT_PREFIX = "inputs/"
BATCH_OUTPUT_PREFIX = "outputs/"
MIN_CHUNK_LEN = 180.0
MAX_CHUNK_LEN = 720.0
GAP_THRESHOLD = 3.0
GPT_MODEL = "gpt-4o-mini"  # Update if using different model

# JSON schema for scene analysis
ENHANCED_SCENE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "scene_id": {"type": "integer"},
        "scene_first_line": {"type": "string"},
        "scene_start_ts_sec": {"type": "number"},
        "content_type": {
            "type": "string",
            "enum": ["program", "commercial", "promo", "unknown"]
        },
        "subject_matter": {"type": "array", "items": {"type": "string"}},
        "mentioned_brands": {"type": "array", "items": {"type": "string"}},
        "commercial_key": {"type": ["string", "null"]},
        "product_genre": {"type": ["string", "null"]},
        "product_subgenre": {"type": ["string", "null"]},
        "call_to_action_present": {"type": ["boolean", "null"]},
        "promo_target_type": {"type": ["string", "null"]},
        "show_key": {"type": ["string", "null"]},
        "show_genre": {
            "type": ["string", "null"],
            "enum": ["news", "sports", "talk", "sitcom", "drama", "reality", "documentary", None]
        },
        "segment_type": {
            "type": ["string", "null"],
            "enum": ["anchor_desk", "interview", "monologue", "field_piece", "panel", "banter", None]
        }
    },
    "required": [
        "scene_id",
        "scene_first_line",
        "scene_start_ts_sec",
        "content_type",
        "subject_matter",
        "mentioned_brands",
        "commercial_key",
        "product_genre",
        "product_subgenre",
        "call_to_action_present",
        "promo_target_type",
        "show_key",
        "show_genre",
        "segment_type"
    ]
}

SCENE_LIST_OBJ_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "scenes": {
            "type": "array",
            "items": ENHANCED_SCENE_SCHEMA
        }
    },
    "required": ["scenes"]
}

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


def save_failed_pushover_message_to_s3(message: str, error_info: dict):
    """Save failed Pushover message to S3 for later review."""
    try:
        timestamp = datetime.datetime.utcnow().isoformat().replace(":", "-")
        key = f"failed-pushover-messages/{timestamp}.json"
        
        data = {
            "original_message": message,
            "error_info": error_info,
            "timestamp": timestamp,
            "source": "BeatwatchWorker"
        }
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        log(f"Saved failed Pushover message to s3://{bucket}/{key}")
        return True
    except Exception as e:
        log(f"Failed to save message to S3: {e}", "ERROR")
        return False


def send_pushover_message(message: str):
    """Send message to Pushover. Returns True if sent successfully, False otherwise."""
    if not pushover_app_token or not pushover_user_key:
        log("Pushover credentials not configured, skipping Pushover notification.")
        return False

    try:
        log(f"Sending Pushover message: {message[:100]}...")
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
        if data.get("status") == 1:
            log("Pushover message sent successfully")
            return True
        else:
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
            log(f"Pushover error: {json.dumps(error_data)}", "ERROR")
            # Save to S3 for recovery
            save_failed_pushover_message_to_s3(message, error_info)
            return False
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
        log(f"Failed to send Pushover message: {json.dumps(error_data)}", "ERROR")
        # Save to S3 for recovery
        save_failed_pushover_message_to_s3(message, error_info)
        return False


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


def chunk_transcript(segments, min_chunk_len=MIN_CHUNK_LEN, max_chunk_len=MAX_CHUNK_LEN, gap_threshold=GAP_THRESHOLD):
    """Chunk transcript segments based on gaps and duration."""
    if not segments:
        return []
    
    chunks, current_chunk = [], [segments[0]]
    current_start = segments[0]['start']
    
    for i in range(len(segments) - 1):
        current_end = segments[i]['end']
        next_seg = segments[i + 1]
        gap = next_seg['start'] - current_end
        duration = current_end - current_start
        
        if (gap > gap_threshold and duration >= min_chunk_len) or duration >= max_chunk_len:
            chunks.append(current_chunk)
            current_chunk = []
            current_start = next_seg['start']
        
        current_chunk.append(next_seg)
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks


def write_prompt_api(segments, output_path):
    """Write a prompt file for API call."""
    header = (
        "You are a precise TV stream analyzer. Your goal is to process transcripts with timestamps and identify distinct scenes.\n\n"
        "### TASK\n"
        "1. Read transcript lines with timestamps.\n"
        "2. Group them into scenes based on the rules below.\n"
        "3. Output **only** the JSON described — no explanations, commentary, or text outside the JSON.\n\n"
        "### SCENE BOUNDARY RULES\n"
        "- Start a new scene if there is ≥3.0 seconds of silence between lines.\n"
        "- Start a new scene if an advertisement or promo begins — anything promoting a product, service, show, network, or app.\n"
        "- Merge consecutive lines that belong to the same continuous scene.\n\n"
        "### OUTPUT FORMAT (JSON only)\n"
        "{\n"
        "  \"scenes\": [\n"
        "    {\n"
        "      \"scene_id\": <int, starting at 1>,\n"
        "      \"scene_first_line\": \"<first line of this scene>\",\n"
        "      \"scene_start_ts_sec\": <float>,\n"
        "      \"content_type\": \"program\" | \"commercial\" | \"promo\" | \"unknown\",\n"
        "      \"subject_matter\": [\"<up to 3 short topics from transcript>\"],\n"
        "      \"mentioned_brands\": [\"<deduplicated, Title Cased brand names>\", ...],\n"
        "      \"commercial_key\": \"<brand or campaign name, or null>\",\n"
        "      \"product_genre\": \"<automotive | beverage | telecom | ... | null>\",\n"
        "      \"product_subgenre\": \"<sports_car | soda | mobile_app | ... | null>\",\n"
        "      \"call_to_action_present\": true | false | null,\n"
        "      \"promo_target_type\": \"<network | show | app | null>\",\n"
        "      \"show_key\": \"<identifier if known, else null>\",\n"
        "      \"show_genre\": \"news\" | \"sports\" | \"talk\" | \"sitcom\" | \"drama\" | \"reality\" | \"documentary\" | null,\n"
        "      \"segment_type\": \"anchor_desk\" | \"interview\" | \"monologue\" | \"field_piece\" | \"panel\" | \"banter\" | null\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "### RULES\n"
        "- Output must be **valid JSON** only.\n"
        "- Use exact property names and allowed values above.\n"
        "- Use float seconds for timestamps (no quotes).\n"
        "- If a value cannot be inferred, use null.\n"
        "- Include no more than 3 short topics in subject_matter.\n"
        "- Mark call_to_action_present as true **only** if there's a clear directive (e.g., 'visit', 'download', 'call now').\n"
        "- Do not include commentary, reasoning, or extra text outside the JSON.\n\n"
        "### INPUT TRANSCRIPT\n"
    )
    
    lines = [header]
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        start = seg.get("start", seg.get("start_offset"))
        text = seg.get("segment") or seg.get("text") or ""
        if start is None:
            continue
        try:
            start = float(start)
        except Exception:
            continue
        text = " ".join(str(text).split())
        lines.append(f"[{start:.1f}] {text}")
    
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


def build_batch_jsonl(prompt_paths, base_name, output_jsonl_path, model=GPT_MODEL):
    """Build batch JSONL file from prompt files."""
    with open(output_jsonl_path, "w", encoding="utf-8") as f:
        for i, p in enumerate(prompt_paths, start=1):
            with open(p, "r", encoding="utf-8") as pf:
                content = pf.read()
            
            item = {
                "custom_id": f"{base_name}_seg{i}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": [{"role": "user", "content": content}],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "scene_list",
                            "schema": SCENE_LIST_OBJ_SCHEMA,
                            "strict": True
                        }
                    }
                }
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def file_exists_in_s3(bucket: str, key: str) -> bool:
    """Check if a file exists in S3."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def rebuild_scenes_from_response(response_key: str) -> dict:
    """
    Rebuild scene analysis from beatwatch_response.json file.
    Parses multiple JSON objects, extracts scenes, combines and sorts them.
    Returns dict with scenes list.
    """
    log(f"  Rebuilding scenes from: s3://{bucket}/{response_key}")
    
    # Download and parse the response file
    obj = s3.get_object(Bucket=bucket, Key=response_key)
    raw = obj["Body"].read().decode("utf-8")
    
    decoder = json.JSONDecoder()
    objs = []
    i = 0
    
    # Parse multiple JSON objects from the file
    while i < len(raw):
        while i < len(raw) and raw[i].isspace():
            i += 1
        if i >= len(raw):
            break
        try:
            o, j = decoder.raw_decode(raw, i)
            objs.append(o)
            i = j
        except json.JSONDecodeError:
            break
    
    # Extract and combine scenes from all responses
    scenes = []
    for o in objs:
        try:
            content_str = o["response"]["body"]["choices"][0]["message"]["content"]
            payload = json.loads(content_str)
            if "scenes" in payload:
                scenes.extend(payload["scenes"])
        except Exception as e:
            log(f"  ⚠️ Error extracting scenes from response object: {e}", "WARNING")
            continue
    
    if not scenes:
        log(f"  ⚠️ No scenes found in response file", "WARNING")
        return {"scenes": []}
    
    # Sort scenes by start timestamp
    scenes.sort(key=lambda s: s.get("scene_start_ts_sec", 0))
    
    # Add scene_id and calculate scene_end_ts_sec
    for idx, s in enumerate(scenes, 1):
        s["scene_id"] = idx
        if idx < len(scenes):
            s["scene_end_ts_sec"] = scenes[idx]["scene_start_ts_sec"]
        else:
            s["scene_end_ts_sec"] = None
    
    total_duration = scenes[-1]["scene_start_ts_sec"] if scenes else 0
    log(f"  ✅ Rebuilt {len(scenes)} scenes | Duration: {total_duration:.1f}s")
    
    return {"scenes": scenes}


def process_transcriptions_for_channel(channel: str):
    """Process all transcription.json files for a channel into batch prompts."""
    log(f"=== Starting transcription processing for channel: {channel} ===")
    
    channel_prefix = f"channels/{channel}/"
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        # List all transcription.json files in the channel folder with pagination
        transcription_keys = []
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=channel_prefix):
            transcription_keys.extend([
                obj['Key'] for obj in page.get('Contents', [])
                if obj['Key'].endswith('transcription.json')
            ])
        
        if not transcription_keys:
            log(f"No transcription.json files found for channel {channel}")
            return {"processed": 0, "skipped": 0, "errors": 0}
        
        log(f"Found {len(transcription_keys)} transcription.json files for channel {channel}")
        
        # Create temporary directory for processing
        temp_dir = tempfile.mkdtemp(prefix="beatwatch_prompt_")
        log(f"Using temporary directory: {temp_dir}")
        
        try:
            for key in transcription_keys:
                try:
                    # Check if beatwatch_prompt.jsonl already exists
                    source_folder = os.path.dirname(key)
                    prompt_key = f"{source_folder}/beatwatch_prompt.jsonl"
                    
                    try:
                        s3.head_object(Bucket=bucket, Key=prompt_key)
                        log(f"  ✓ Prompt already exists: s3://{bucket}/{prompt_key} - SKIPPING")
                        skipped_count += 1
                        continue
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "404":
                            log(f"  ✗ Error checking existing prompt: {e}", "ERROR")
                            raise
                    
                    # Download and process transcription
                    log(f"  Processing: {key}")
                    obj = s3.get_object(Bucket=bucket, Key=key)
                    data = json.load(obj['Body'])
                    segments = data.get('segments', [])
                    
                    if not segments:
                        log(f"  ⚠️ No segments found in {key} - SKIPPING")
                        skipped_count += 1
                        continue
                    
                    base_name = os.path.basename(os.path.dirname(key))
                    
                    # Chunk the transcript
                    chunks = chunk_transcript(segments)
                    log(f"  Created {len(chunks)} chunks from transcription")
                    
                    # Create prompt files
                    prompt_paths = []
                    for i, chunk in enumerate(chunks, start=1):
                        prompt_path = os.path.join(temp_dir, f"{base_name}_seg{i}_prompt.txt")
                        write_prompt_api(chunk, prompt_path)
                        prompt_paths.append(prompt_path)
                    
                    # Build batch JSONL
                    batch_jsonl_path = os.path.join(temp_dir, f"{base_name}_batch_input.jsonl")
                    build_batch_jsonl(prompt_paths, base_name, batch_jsonl_path)
                    
                    # Upload to input bucket
                    input_bucket_key = f"{BATCH_INPUT_PREFIX}{os.path.basename(batch_jsonl_path)}"
                    s3.upload_file(batch_jsonl_path, BATCH_INPUT_BUCKET, input_bucket_key)
                    log(f"  ✅ Uploaded to s3://{BATCH_INPUT_BUCKET}/{input_bucket_key}")
                    
                    # Upload to original folder
                    s3.upload_file(batch_jsonl_path, bucket, prompt_key)
                    log(f"  ✅ Uploaded to s3://{bucket}/{prompt_key}")
                    
                    processed_count += 1
                    
                except Exception as e:
                    log(f"  ❌ Error processing {key}: {e}", "ERROR")
                    import traceback
                    log(f"  Traceback: {traceback.format_exc()}", "ERROR")
                    error_count += 1
                    continue
        
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir)
                log(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                log(f"Warning: Failed to clean up temp directory {temp_dir}: {e}", "WARNING")
        
        log(f"=== Transcription processing complete for {channel}: processed={processed_count}, skipped={skipped_count}, errors={error_count} ===")
        return {"processed": processed_count, "skipped": skipped_count, "errors": error_count}
        
    except Exception as e:
        log(f"ERROR in transcription processing for channel {channel}: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return {"error": str(e), "processed": processed_count, "skipped": skipped_count, "errors": error_count}


def process_batch_outputs_for_channel(channel: str):
    """Process batch processor outputs: copy to beatwatch folders and rebuild scenes."""
    log(f"=== Starting batch output processing for channel: {channel} ===")
    
    channel_prefix = f"channels/{channel}/"
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        # List all segment folders by finding transcription.json files
        transcription_keys = []
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=channel_prefix):
            transcription_keys.extend([
                obj['Key'] for obj in page.get('Contents', [])
                if obj['Key'].endswith('transcription.json')
            ])
        
        if not transcription_keys:
            log(f"No segment folders found for channel {channel}")
            return {"processed": 0, "skipped": 0, "errors": 0}
        
        log(f"Found {len(transcription_keys)} segment folders for channel {channel}")
        
        for key in transcription_keys:
            try:
                # Extract segment folder name
                source_folder = os.path.dirname(key)
                folder_name = os.path.basename(source_folder)
                
                # Check if beatwatch_scenes.json already exists
                scenes_key = f"{source_folder}/beatwatch_scenes.json"
                try:
                    s3.head_object(Bucket=bucket, Key=scenes_key)
                    log(f"  ✓ Scenes already exist: s3://{bucket}/{scenes_key} - SKIPPING")
                    skipped_count += 1
                    continue
                except ClientError as e:
                    if e.response["Error"]["Code"] != "404":
                        log(f"  ✗ Error checking existing scenes: {e}", "ERROR")
                        raise
                
                # Check for output file in batch processor outputs
                output_key = f"{BATCH_OUTPUT_PREFIX.rstrip('/')}/{folder_name}_batch_input.jsonl"
                
                if not file_exists_in_s3(BATCH_INPUT_BUCKET, output_key):
                    log(f"  ⚠️ Output not found: s3://{BATCH_INPUT_BUCKET}/{output_key} - SKIPPING")
                    skipped_count += 1
                    continue
                
                log(f"  Processing: {folder_name}")
                
                # Copy output file to beatwatch folder as beatwatch_response.json
                response_key = f"{source_folder}/beatwatch_response.json"
                
                # Download from outputs bucket
                output_obj = s3.get_object(Bucket=BATCH_INPUT_BUCKET, Key=output_key)
                response_data = output_obj["Body"].read()
                
                # Upload to beatwatch folder
                s3.put_object(
                    Bucket=bucket,
                    Key=response_key,
                    Body=response_data
                )
                log(f"  ✅ Copied to s3://{bucket}/{response_key}")
                
                # Delete from outputs bucket
                s3.delete_object(Bucket=BATCH_INPUT_BUCKET, Key=output_key)
                log(f"  ✅ Deleted from s3://{BATCH_INPUT_BUCKET}/{output_key}")
                
                # Rebuild scenes from response
                scenes_data = rebuild_scenes_from_response(response_key)
                
                # Save scenes as beatwatch_scenes.json
                scenes_json = json.dumps(scenes_data, indent=2, ensure_ascii=False)
                s3.put_object(
                    Bucket=bucket,
                    Key=scenes_key,
                    Body=scenes_json.encode('utf-8'),
                    ContentType='application/json'
                )
                log(f"  ✅ Saved scenes to s3://{bucket}/{scenes_key}")
                
                processed_count += 1
                
            except Exception as e:
                log(f"  ❌ Error processing {key}: {e}", "ERROR")
                import traceback
                log(f"  Traceback: {traceback.format_exc()}", "ERROR")
                error_count += 1
                continue
        
        log(f"=== Batch output processing complete for {channel}: processed={processed_count}, skipped={skipped_count}, errors={error_count} ===")
        return {"processed": processed_count, "skipped": skipped_count, "errors": error_count}
        
    except Exception as e:
        log(f"ERROR in batch output processing for channel {channel}: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return {"error": str(e), "processed": processed_count, "skipped": skipped_count, "errors": error_count}


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

        log(f"=== Channel {channel} transcription queueing complete: sent={sent}, skipped={skipped} ===")
        
        # Process existing transcriptions into batch prompts
        transcription_result = process_transcriptions_for_channel(channel)
        
        # Process batch outputs: copy to beatwatch folders and rebuild scenes
        batch_output_result = process_batch_outputs_for_channel(channel)
        
        return {
            "sent": sent, 
            "skipped": skipped, 
            "channel": channel,
            "transcriptions_processed": transcription_result.get("processed", 0),
            "transcriptions_skipped": transcription_result.get("skipped", 0),
            "transcription_errors": transcription_result.get("errors", 0),
            "batch_outputs_processed": batch_output_result.get("processed", 0),
            "batch_outputs_skipped": batch_output_result.get("skipped", 0),
            "batch_output_errors": batch_output_result.get("errors", 0)
        }

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
    total_transcriptions_processed = 0
    total_transcriptions_skipped = 0
    total_transcription_errors = 0
    total_batch_outputs_processed = 0
    total_batch_outputs_skipped = 0
    total_batch_output_errors = 0
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
                    total_transcriptions_processed += result.get("transcriptions_processed", 0)
                    total_transcriptions_skipped += result.get("transcriptions_skipped", 0)
                    total_transcription_errors += result.get("transcription_errors", 0)
                    total_batch_outputs_processed += result.get("batch_outputs_processed", 0)
                    total_batch_outputs_skipped += result.get("batch_outputs_skipped", 0)
                    total_batch_output_errors += result.get("batch_output_errors", 0)
                    log(f"Channel {channel} processed successfully. Running totals: sent={total_sent}, skipped={total_skipped}, transcriptions processed={total_transcriptions_processed}, batch outputs processed={total_batch_outputs_processed}")
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
        f"Total transcriptions processed into batch prompts: {total_transcriptions_processed}\n"
        f"Total transcriptions skipped (already processed): {total_transcriptions_skipped}\n"
        f"Transcription processing errors: {total_transcription_errors}\n"
        f"Total batch outputs processed: {total_batch_outputs_processed}\n"
        f"Total batch outputs skipped (already processed): {total_batch_outputs_skipped}\n"
        f"Batch output processing errors: {total_batch_output_errors}\n"
    )
    
    log("Final summary:")
    log(f"  Instance ID: {instance_id or 'unknown'}")
    log(f"  ASG Name: {asg_name or 'unknown'}")
    log(f"  Channels processed: {len(channels_processed)}")
    log(f"  Channels list: {', '.join(channels_processed) if channels_processed else '(none)'}")
    log(f"  Total files sent to transcribe queue: {total_sent}")
    log(f"  Total files skipped (already processed): {total_skipped}")
    log(f"  Total transcriptions processed into batch prompts: {total_transcriptions_processed}")
    log(f"  Total transcriptions skipped (already processed): {total_transcriptions_skipped}")
    log(f"  Transcription processing errors: {total_transcription_errors}")
    log(f"  Total batch outputs processed: {total_batch_outputs_processed}")
    log(f"  Total batch outputs skipped (already processed): {total_batch_outputs_skipped}")
    log(f"  Batch output processing errors: {total_batch_output_errors}")
    log(f"  Total poll attempts: {poll_count}")
    
    if send_pushover_message(msg):
        log("Summary sent to Pushover")
    else:
        log("Pushover summary skipped (credentials not configured)")
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
    except SystemExit as e:
        # Update exit code from sys.exit() calls
        _exit_code[0] = e.code if e.code is not None else 0
        raise  # Re-raise to allow normal exit
    except Exception as e:
        _exit_code[0] = 1
        log(f"Uncaught exception in main: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        sys.exit(1)

