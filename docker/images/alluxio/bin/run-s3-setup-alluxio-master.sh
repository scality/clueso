#!/usr/bin/env bash
#
echo "Sleeping for 10 seconds to give time to S3 to come up"
sleep 10;

echo "Creating METADATA bucket in S3 (ENDPOINT = $ALLUXIO_UNDERFS_S3_ENDPOINT )"
aws s3api create-bucket --bucket METADATA --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

echo "Setting bucket ACLs (ENDPOINT = $ALLUXIO_UNDERFS_S3_ENDPOINT )"
aws s3api put-bucket-acl --bucket METADATA --acl public-read-write --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

echo "Creating /alluxio on S3"
aws s3api put-object --bucket METADATA --key alluxio/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

echo "Setting permissions"
aws s3api put-object-acl --bucket METADATA --key alluxio --acl public-read-write --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

echo "Deleting landing and staging if existent"
aws s3 rm --recursive s3://METADATA/alluxio/landing --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT
aws s3 rm --recursive s3://METADATA/alluxio/staging --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

#aws s3api put-object --bucket METADATA --key alluxio/landing/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT
#aws s3api put-object --bucket METADATA --key alluxio/staging/.ignore --endpoint-url $ALLUXIO_UNDERFS_S3_ENDPOINT

/entrypoint.sh master