package tests

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"floci-sdk-test-go/internal/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3(t *testing.T) {
	ctx := context.Background()
	svc := testutil.S3Client()
	bucket := "go-test-bucket"
	key := "test-object.json"
	content := `{"source":"go-test"}`

	// Cleanup at end
	t.Cleanup(func() {
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy-" + key)})
		svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	t.Run("CreateBucket", func(t *testing.T) {
		_, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
	})

	t.Run("ListBuckets", func(t *testing.T) {
		r, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, r.Buckets)
	})

	t.Run("PutObject", func(t *testing.T) {
		_, err := svc.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(content),
			ContentType: aws.String("application/json"),
		})
		require.NoError(t, err)
	})

	t.Run("GetObject", func(t *testing.T) {
		out, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer out.Body.Close()

		var buf bytes.Buffer
		buf.ReadFrom(out.Body)
		assert.Equal(t, content, buf.String())
	})

	t.Run("HeadObject", func(t *testing.T) {
		head, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.NotNil(t, head.LastModified)
		assert.Zero(t, head.LastModified.Nanosecond(), "LastModified should have second precision")
	})

	t.Run("ListObjectsV2", func(t *testing.T) {
		r, err := svc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		assert.NotEmpty(t, r.Contents)
	})

	t.Run("CopyObject", func(t *testing.T) {
		_, err := svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			CopySource: aws.String(bucket + "/" + key),
			Key:        aws.String("copy-" + key),
		})
		require.NoError(t, err)
	})

	t.Run("DeleteObject", func(t *testing.T) {
		_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		// First delete the copy
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("copy-" + key),
		})

		_, err := svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
	})
}

func TestS3LocationConstraint(t *testing.T) {
	ctx := context.Background()
	svc := testutil.S3Client()
	euBucket := "go-test-bucket-eu"

	t.Cleanup(func() {
		svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(euBucket)})
	})

	t.Run("CreateBucketWithLocationConstraint", func(t *testing.T) {
		_, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(euBucket),
			CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraintEuCentral1,
			},
		})
		require.NoError(t, err)
	})

	t.Run("GetBucketLocation", func(t *testing.T) {
		loc, err := svc.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(euBucket)})
		require.NoError(t, err)
		assert.Equal(t, "eu-central-1", string(loc.LocationConstraint))
	})
}
