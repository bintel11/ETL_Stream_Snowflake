"""
S3 client for uploading and downloading files.
"""

import boto3
import os

class S3Client:
    def __init__(self):
        self.s3 = boto3.client("s3")
        self.bucket = os.getenv("S3_BUCKET")

    def upload_file(self, file_path: str, key: str):
        """Upload local file to S3 bucket."""
        self.s3.upload_file(file_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def download_file(self, key: str, dest_path: str):
        """Download file from S3 to local path."""
        self.s3.download_file(self.bucket, key, dest_path)
        return dest_path
