import boto3
import logging
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

def parse_args():
    parser = argparse.ArgumentParser(description="Check S3 Glacier restore status for objects by prefix.")
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', required=True, help='S3 prefix (folder)')
    parser.add_argument('--log-file', default='glacier_restore_status.log', help='Log file name')
    parser.add_argument('--threads', type=int, default=10, help='Number of parallel threads for status check')
    return parser.parse_args()

def check_restore_status(s3, bucket, key):
    try:
        obj = s3.head_object(Bucket=bucket, Key=key)
        restore_field = obj.get('Restore')
        size = obj.get('ContentLength', 0)
        if restore_field:
            if 'ongoing-request="false"' in restore_field:
                return key, 'Completed', size
            elif 'ongoing-request="true"' in restore_field:
                return key, 'In Progress', size
        return key, 'Not Started', size
    except Exception as e:
        logging.error(f"Error checking {key}: {e}")
        return key, 'Error', 0

def summarize_restore_status(args):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=args.bucket, Prefix=args.prefix)

    summary = {
        'Completed': 0,
        'In Progress': 0,
        'Not Started': 0,
        'Error': 0,
        'Total': 0,
        'RestoredSize': 0,
        'TotalSize': 0,
    }
    objects = []

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            size = obj.get('Size', 0)
            if key == args.prefix or size == 0 or 'StorageClass' not in obj:
                continue
            objects.append((key, size))

    summary['Total'] = len(objects)
    summary['TotalSize'] = sum(size for _, size in objects)

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(check_restore_status, s3, args.bucket, key) for key, _ in objects]
        for future in as_completed(futures):
            key, status, restored_bytes = future.result()
            summary[status] += 1
            if status == 'Completed':
                summary['RestoredSize'] += restored_bytes
            logging.info(f"{key}: {status}")

    percent_complete = (summary['Completed'] / summary['Total']) * 100 if summary['Total'] else 0

    print("\n===== Glacier Restore Status Summary =====")
    print(f"Total objects scanned:   {summary['Total']}")
    print(f"Restore Completed:       {summary['Completed']}")
    print(f"Restore In Progress:     {summary['In Progress']}")
    print(f"Restore Not Started:     {summary['Not Started']}")
    print(f"Errors:                  {summary['Error']}")
    print(f"Percent Completed:       {percent_complete:.2f}%")
    print(f"Total Data Size:         {summary['TotalSize'] / (1024**3):.2f} GB")
    print(f"Total Restored Size:     {summary['RestoredSize'] / (1024**3):.2f} GB")
    print(f"\nDetails logged to {args.log_file}")

if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(args.log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    summarize_restore_status(args)
