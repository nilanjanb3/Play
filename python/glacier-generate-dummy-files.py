"""Generate dummy files and upload them to S3 using Glacier storage class."""

import os
import boto3
import random
import string
from concurrent.futures import ThreadPoolExecutor

BUCKET_NAME = 'dump1425bucket'
PREFIX = 'images/'
NUM_FILES = 10000
FILE_SIZE_BYTES = 1024  # 1 KB per file (customize as needed)
LOCAL_DIR = './dummy_files'

# Step 1: Generate dummy files
os.makedirs(LOCAL_DIR, exist_ok=True)
for i in range(1, NUM_FILES + 1):
    filename = f'dummy_file_{i:04d}.txt'
    filepath = os.path.join(LOCAL_DIR, filename)
    content = ''.join(random.choices(string.ascii_letters + string.digits, k=FILE_SIZE_BYTES))
    with open(filepath, 'w') as f:
        f.write(content)

print(f"✅ Generated {NUM_FILES} dummy files in {LOCAL_DIR}")

# Step 2: Upload files to S3 Glacier Flexible Retrieval tier
s3 = boto3.client('s3')

def upload_file(filepath):
    """Upload a file to S3 using the Glacier storage class.

    Parameters
    ----------
    filepath : str
        Path to the file that will be uploaded.
    """
    filename = os.path.basename(filepath)
    s3_key = f"{PREFIX}{filename}"
    try:
        s3.upload_file(
            Filename=filepath,
            Bucket=BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={'StorageClass': 'GLACIER'}
        )
        print(f"Uploaded: {s3_key}")
    except Exception as e:
        print(f"Failed: {s3_key} – {e}")

filepaths = [os.path.join(LOCAL_DIR, f) for f in os.listdir(LOCAL_DIR)]
# Use a thread pool for faster upload
with ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(upload_file, filepaths)

print("✅ All files uploaded to S3 with Glacier Flexible Retrieval storage class.")

# Step 3: Clean up local files (optional)
import shutil
shutil.rmtree(LOCAL_DIR)
print("🧹 Cleaned up local dummy files.")
