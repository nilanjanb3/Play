import boto3
import logging
import argparse
import sys
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

def parse_args():
    parser = argparse.ArgumentParser(description="Restore S3 Glacier objects by prefix and show detailed status.")
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', required=True, help='S3 prefix (folder)')
    parser.add_argument('--log-file', default='glacier_restore.log', help='Log file name')
    parser.add_argument('--days', type=int, default=2, help='Number of days to keep restored objects')
    parser.add_argument('--tier', default='Expedited', choices=['Expedited', 'Standard', 'Bulk'], help='Restore tier')
    parser.add_argument('--threads', type=int, default=10, help='Number of parallel threads')
    return parser.parse_args()

def check_and_restore_object(s3, bucket, key, days, tier):
    # Step 1: Check current restore status
    try:
        obj = s3.head_object(Bucket=bucket, Key=key)
        restore_field = obj.get('Restore')
        storage_class = obj.get('StorageClass')
        if storage_class not in ['GLACIER', 'DEEP_ARCHIVE', 'GLACIER_IR']:
            # Not a Glacier object (or already restored to Standard/IA)
            logging.info(f"{key}: Not eligible for restore (StorageClass: {storage_class})")
            return key, 'Not Eligible', None
        if restore_field:
            if 'ongoing-request="true"' in restore_field:
                logging.info(f"{key}: Restore already in progress.")
                return key, 'In Progress', tier
            elif 'ongoing-request="false"' in restore_field:
                logging.info(f"{key}: Restore already completed.")
                return key, 'Completed', tier
    except Exception as e:
        logging.warning(f"head_object failed for {key}: {e}")

    # Step 2: Try to restore (first with user's tier, then fallback)
    try:
        s3.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={
                'Days': days,
                'GlacierJobParameters': {'Tier': tier}
            }
        )
        logging.info(f"{key}: Restore requested ({tier}).")
        return key, 'Requested', tier
    except ClientError as e:
        error_str = str(e)
        if ("rate of expedited retrievals" in error_str or "is not allowed" in error_str or 
            "cannot be expedited" in error_str):
            # Fallback to Standard tier
            try:
                s3.restore_object(
                    Bucket=bucket,
                    Key=key,
                    RestoreRequest={
                        'Days': days,
                        'GlacierJobParameters': {'Tier': 'Standard'}
                    }
                )
                logging.warning(f"{key}: Expedited not available, switched to Standard.")
                return key, 'Requested', 'Standard'
            except Exception as ex:
                logging.error(f"{key}: Failed to restore with Standard tier: {ex}")
                return key, 'Failed', 'Standard'
        elif "already in progress" in error_str:
            logging.info(f"{key}: Restore already in progress (detected from exception).")
            return key, 'In Progress', tier
        elif "RestoreAlreadyInProgress" in error_str:
            logging.info(f"{key}: Restore already in progress (RestoreAlreadyInProgress).")
            return key, 'In Progress', tier
        elif "Object restore is not allowed" in error_str:
            logging.warning(f"{key}: Object restore is not allowed (perhaps not in Glacier).")
            return key, 'Not Eligible', None
        else:
            logging.error(f"{key}: Restore failed: {e}")
            return key, 'Failed', tier

def list_glacier_objects(s3, bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    objects = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            size = obj.get('Size', 0)
            if key == prefix or size == 0 or 'StorageClass' not in obj:
                continue
            # Only GLACIER/GLACIER_IR/DEEP_ARCHIVE objects require restore
            if obj.get('StorageClass') in ['GLACIER', 'DEEP_ARCHIVE', 'GLACIER_IR']:
                objects.append((key, size))
    return objects

def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(args.log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    s3 = boto3.client('s3')
    objects = list_glacier_objects(s3, args.bucket, args.prefix)
    total_size = sum(size for _, size in objects)
    status_counts = Counter()
    tier_usage = Counter()
    print(f"Scanning {len(objects)} objects under {args.prefix} ...\n")

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [
            executor.submit(check_and_restore_object, s3, args.bucket, key, args.days, args.tier)
            for key, _ in objects
        ]
        for future in as_completed(futures):
            key, status, tier_used = future.result()
            status_counts[status] += 1
            if tier_used:
                tier_usage[tier_used] += 1
            print(f"{key}: {status}{f' (Tier: {tier_used})' if tier_used else ''}")

    print("\n===== Glacier Restore Status Summary =====")
    print(f"Total objects scanned:        {len(objects)}")
    print(f"Total data size (GB):         {total_size / (1024**3):.2f}")
    for status in ['Completed', 'In Progress', 'Requested', 'Not Eligible', 'Failed']:
        print(f"{status + ':':25} {status_counts[status]}")
    print("Tier usage (for restore requests):")
    for tier in tier_usage:
        print(f"  {tier}: {tier_usage[tier]}")

    print(f"\nDetails logged to {args.log_file}")

if __name__ == "__main__":
    main()
