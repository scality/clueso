#!/usr/bin/env bash
#
echo "Sleeping for 10 seconds to give time to S3 to come up"
sleep 10;

echo "Creating METADATA bucket in S3 (ENDPOINT = $ALLUXIO_UNDERFS_S3_ENDPOINT )"
aws s3api create-bucket --bucket METADATA --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT
aws s3api put-bucket-acl --bucket METADATA --acl public-read-write --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT
aws s3api put-object --bucket METADATA --key alluxio/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT


#aws s3api put-object --bucket METADATA --key alluxio/landing/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT
#aws s3api put-object --bucket METADATA --key alluxio/staging/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

/entrypoint.sh master