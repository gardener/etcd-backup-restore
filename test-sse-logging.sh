#!/bin/bash

# Test script to verify SSE logging in dual snapstore

export STORAGE_PROVIDER=S3
export STORAGE_CONTAINER=my-bucket

# Primary endpoint configuration (will fail)
export AWS_APPLICATION_CREDENTIALS_JSON='{"endpoint":"https://doesnotexist.com","region":"us-east-1","accessKeyID":"test","secretAccessKey":"test","bucketName":"my-bucket","SSECustomerKey":"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ==","SSECustomerAlgorithm":"AES256"}'

# Secondary endpoint configuration (real endpoint)
export SECONDARY_STORAGE_CONTAINER=etcd-backup-restore-e2e-test
export SECONDARY_AWS_APPLICATION_CREDENTIALS_JSON='{"endpoint":"https://s3.amazonaws.com","region":"us-east-1","accessKeyID":"'$AWS_ACCESS_KEY_ID'","secretAccessKey":"'$AWS_SECRET_ACCESS_KEY'","bucketName":"etcd-backup-restore-e2e-test","SSECustomerKey":"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ==","SSECustomerAlgorithm":"AES256"}'

# Set debug logging
export LOG_LEVEL=debug

# Create a test etcd data directory
mkdir -p /tmp/etcd-test
echo '{"key":"value"}' > /tmp/etcd-test/test.db

echo "Testing SSE logging with dual snapstore..."
echo "Primary endpoint: https://doesnotexist.com (will fail)"
echo "Secondary endpoint: https://s3.amazonaws.com (should succeed)"
echo ""

# Run the backup command to trigger SSE credential processing
./etcd-backup-restore \
  --etcd-data-dir=/tmp/etcd-test \
  --delta-snapshot-period=1s \
  --delta-snapshot-memory-limit=1Mi \
  --garbage-collection-period=1m \
  --schedule="*/1 * * * *" \
  --max-backups=1 \
  --storage-provider=S3 \
  server 2>&1 | grep -E "(SSE|primary|secondary|Applying|Processing|Generated)"

echo ""
echo "Look for logs showing SSE credential processing for both [primary] and [secondary] endpoints"