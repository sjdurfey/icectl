#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.34.0",
#     "botocore>=1.34.0"
# ]
# ///

"""Diagnose RustFS S3 bucket policies and permissions."""

import boto3
import json
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

# RustFS configuration
ENDPOINT_URL = "http://10.0.0.166:9090"  # External endpoint
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin123"
REGION = "us-east-1"

def create_s3_client():
    """Create S3 client for RustFS."""
    config = Config(
        region_name=REGION,
        signature_version='s3v4',
        s3={
            'addressing_style': 'path'
        }
    )

    return boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=config
    )

def list_buckets(s3_client):
    """List all buckets."""
    try:
        response = s3_client.list_buckets()
        print("✅ Successfully connected to RustFS")
        print("📦 Available buckets:")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']} (created: {bucket['CreationDate']})")
        return [bucket['Name'] for bucket in response['Buckets']]
    except ClientError as e:
        print(f"❌ Error listing buckets: {e}")
        return []
    except NoCredentialsError:
        print("❌ No credentials available")
        return []

def check_bucket_policy(s3_client, bucket_name):
    """Check bucket policy for a given bucket."""
    print(f"\n🔍 Checking bucket policy for: {bucket_name}")

    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(response['Policy'])
        print(f"📋 Bucket policy exists:")
        print(json.dumps(policy, indent=2))
        return policy
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucketPolicy':
            print(f"ℹ️  No bucket policy set (uses default permissions)")
        else:
            print(f"❌ Error getting bucket policy: {e}")
        return None

def check_bucket_acl(s3_client, bucket_name):
    """Check bucket ACL."""
    print(f"\n🔐 Checking ACL for: {bucket_name}")

    try:
        response = s3_client.get_bucket_acl(Bucket=bucket_name)
        print(f"👤 Owner: {response['Owner']}")
        print(f"🎫 Grants:")
        for grant in response['Grants']:
            grantee = grant['Grantee']
            permission = grant['Permission']
            if 'DisplayName' in grantee:
                print(f"  - {grantee['DisplayName']}: {permission}")
            elif 'URI' in grantee:
                print(f"  - {grantee['URI']}: {permission}")
            elif 'ID' in grantee:
                print(f"  - {grantee['ID']}: {permission}")
        return response
    except ClientError as e:
        print(f"❌ Error getting bucket ACL: {e}")
        return None

def test_bucket_operations(s3_client, bucket_name):
    """Test basic read/write operations on bucket."""
    print(f"\n🧪 Testing operations on: {bucket_name}")

    test_key = "test-iceberg-access/test.txt"
    test_content = "Test content from iceberg-viewer"

    # Test write
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content,
            ContentType='text/plain'
        )
        print(f"✅ Write test successful")

        # Test read
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        if content == test_content:
            print(f"✅ Read test successful")
        else:
            print(f"⚠️  Read test failed - content mismatch")

        # Test delete
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ Delete test successful")

        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"❌ Operation failed: {error_code} - {e}")
        return False

def check_bucket_location(s3_client, bucket_name):
    """Check bucket location/region."""
    try:
        response = s3_client.get_bucket_location(Bucket=bucket_name)
        location = response.get('LocationConstraint', 'us-east-1')  # Default for us-east-1
        print(f"🌍 Bucket location: {location or 'us-east-1 (default)'}")
        return location
    except ClientError as e:
        print(f"❌ Error getting bucket location: {e}")
        return None

def main():
    """Main diagnostic function."""
    print("🔧 RustFS S3 Bucket Diagnostics")
    print("=" * 50)
    print(f"🔗 Endpoint: {ENDPOINT_URL}")
    print(f"🔑 Access Key: {ACCESS_KEY}")
    print(f"🌎 Region: {REGION}")

    # Create S3 client
    s3_client = create_s3_client()

    # List buckets
    buckets = list_buckets(s3_client)

    if not buckets:
        print("❌ No buckets found or connection failed")
        return

    # Analyze each bucket that Lakekeeper might use
    target_buckets = ['lakehouse', 'data']
    available_target_buckets = [b for b in target_buckets if b in buckets]

    if not available_target_buckets:
        print(f"⚠️  Target buckets {target_buckets} not found in available buckets: {buckets}")
        return

    for bucket_name in available_target_buckets:
        print(f"\n{'='*60}")
        print(f"🔍 ANALYZING BUCKET: {bucket_name}")
        print('='*60)

        # Check bucket location
        check_bucket_location(s3_client, bucket_name)

        # Check bucket policy
        check_bucket_policy(s3_client, bucket_name)

        # Check bucket ACL
        check_bucket_acl(s3_client, bucket_name)

        # Test operations
        operations_work = test_bucket_operations(s3_client, bucket_name)

        if operations_work:
            print(f"✅ {bucket_name} bucket is fully accessible!")
        else:
            print(f"❌ {bucket_name} bucket has permission issues")

    print(f"\n{'='*60}")
    print("📋 SUMMARY")
    print('='*60)

    working_buckets = []
    for bucket_name in available_target_buckets:
        print(f"Testing {bucket_name} again for final verification...")
        if test_bucket_operations(s3_client, bucket_name):
            working_buckets.append(bucket_name)

    if working_buckets:
        print(f"✅ Working buckets for Lakekeeper: {working_buckets}")
        print(f"💡 Use one of these buckets when creating the warehouse")
    else:
        print("❌ No working buckets found - check RustFS configuration")

if __name__ == "__main__":
    main()