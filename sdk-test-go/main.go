// Floci SDK Test — Go (AWS SDK v2)
//
// Runs against the Floci AWS emulator. Configure via:
//
//	FLOCI_ENDPOINT=http://localhost:4566 (default)
//
// To run specific groups:
//
//	/floci-test ssm sqs s3
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/url"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

var (
	passed int
	failed int
	ctx    = context.Background()
)

func check(name string, err error, cond ...bool) {
	ok := err == nil
	if ok && len(cond) > 0 {
		ok = cond[0]
	}
	if ok {
		passed++
		fmt.Printf("  PASS  %s\n", name)
	} else {
		failed++
		if err != nil {
			fmt.Printf("  FAIL  %s — %v\n", name, err)
		} else {
			fmt.Printf("  FAIL  %s\n", name)
		}
	}
}

func newCfg(endpoint string) aws.Config {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AWS config: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

func minimalZip() []byte {
	code := `exports.handler = async (event) => {
  const name = (event && event.name) ? event.name : "World";
  return { statusCode: 200, body: JSON.stringify({ message: "Hello, " + name + "!" }) };
};`
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, _ := w.Create("index.js")
	f.Write([]byte(code))
	w.Close()
	return buf.Bytes()
}

// ── SSM ───────────────────────────────────────────────────────────────────────

func runSSM(cfg aws.Config) {
	fmt.Println("--- SSM Tests ---")
	svc := ssm.NewFromConfig(cfg)
	name := "/go-sdk-test/param"
	value := "go-sdk-value"

	_, err := svc.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(name),
		Value:     aws.String(value),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	check("SSM PutParameter", err)

	r, err := svc.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name)})
	check("SSM GetParameter", err, err == nil && aws.ToString(r.Parameter.Value) == value)

	gr, err := svc.GetParameters(ctx, &ssm.GetParametersInput{Names: []string{name}})
	check("SSM GetParameters", err, err == nil && len(gr.Parameters) == 1)

	dr, err := svc.DescribeParameters(ctx, &ssm.DescribeParametersInput{})
	check("SSM DescribeParameters", err, err == nil && len(dr.Parameters) > 0)

	pr, err := svc.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
		Path: aws.String("/go-sdk-test"),
	})
	check("SSM GetParametersByPath", err, err == nil && len(pr.Parameters) > 0)

	_, err = svc.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: aws.String(name)})
	check("SSM DeleteParameter", err)
}

// ── SQS ───────────────────────────────────────────────────────────────────────

func runSQS(cfg aws.Config) {
	fmt.Println("--- SQS Tests ---")
	svc := sqs.NewFromConfig(cfg)
	qName := "go-sdk-test-queue"

	cr, err := svc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(qName)})
	check("SQS CreateQueue", err)
	if err != nil {
		return
	}
	qURL := aws.ToString(cr.QueueUrl)

	lr, err := svc.ListQueues(ctx, &sqs.ListQueuesInput{})
	check("SQS ListQueues", err, err == nil && len(lr.QueueUrls) > 0)

	ur, err := svc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(qName)})
	check("SQS GetQueueUrl", err, err == nil && aws.ToString(ur.QueueUrl) == qURL)

	ar, err := svc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(qURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	check("SQS GetQueueAttributes", err, err == nil && ar.Attributes["QueueArn"] != "")

	sm, err := svc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"source":"go-sdk"}`),
	})
	check("SQS SendMessage", err, err == nil && aws.ToString(sm.MessageId) != "")

	bmr, err := svc.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(qURL),
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{Id: aws.String("m1"), MessageBody: aws.String("batch-msg-1")},
			{Id: aws.String("m2"), MessageBody: aws.String("batch-msg-2")},
		},
	})
	check("SQS SendMessageBatch", err, err == nil && len(bmr.Successful) == 2)

	recv, err := svc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	check("SQS ReceiveMessage", err, err == nil && len(recv.Messages) > 0)

	if len(recv.Messages) > 0 {
		_, err = svc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(qURL),
			ReceiptHandle: recv.Messages[0].ReceiptHandle,
		})
		check("SQS DeleteMessage", err)
	}

	_, err = svc.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(qURL),
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	check("SQS SetQueueAttributes", err)

	_, err = svc.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(qURL)})
	check("SQS PurgeQueue", err)

	_, err = svc.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(qURL)})
	check("SQS DeleteQueue", err)
}

// ── SNS ───────────────────────────────────────────────────────────────────────

func runSNS(cfg aws.Config) {
	fmt.Println("--- SNS Tests ---")
	svc := sns.NewFromConfig(cfg)
	sqsSvc := sqs.NewFromConfig(cfg)
	topicName := "go-sdk-test-topic"

	ct, err := svc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	check("SNS CreateTopic", err)
	if err != nil {
		return
	}
	topicARN := aws.ToString(ct.TopicArn)

	lt, err := svc.ListTopics(ctx, &sns.ListTopicsInput{})
	check("SNS ListTopics", err, err == nil && len(lt.Topics) > 0)

	ga, err := svc.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topicARN)})
	check("SNS GetTopicAttributes", err, err == nil && ga.Attributes["TopicArn"] == topicARN)

	// Create a target SQS queue and subscribe
	sqr, sqErr := sqsSvc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("go-sdk-sns-target")})
	var subARN string
	if sqErr == nil {
		sqAttr, _ := sqsSvc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       sqr.QueueUrl,
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		})
		sr, err := svc.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: aws.String(topicARN),
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(sqAttr.Attributes["QueueArn"]),
		})
		check("SNS Subscribe", err)
		if err == nil {
			subARN = aws.ToString(sr.SubscriptionArn)
		}
	}

	ls, err := svc.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	check("SNS ListSubscriptions", err, err == nil && len(ls.Subscriptions) > 0)

	pub, err := svc.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String(`{"event":"go-sdk-test"}`),
		Subject:  aws.String("GoSDKTest"),
	})
	check("SNS Publish", err, err == nil && aws.ToString(pub.MessageId) != "")

	if subARN != "" {
		_, err = svc.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{
			SubscriptionArn: aws.String(subARN),
		})
		check("SNS GetSubscriptionAttributes", err)

		_, err = svc.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(subARN)})
		check("SNS Unsubscribe", err)
	}

	if sqErr == nil {
		sqsSvc.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: sqr.QueueUrl})
	}

	_, err = svc.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	check("SNS DeleteTopic", err)
}

// ── S3 ────────────────────────────────────────────────────────────────────────

func runS3(cfg aws.Config) {
	fmt.Println("--- S3 Tests ---")
	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	bucket := "go-sdk-test-bucket"
	key := "test-object.json"
	content := `{"source":"go-sdk-test"}`

	_, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	check("S3 CreateBucket", err)
	if err != nil {
		return
	}

	// CreateBucket with LocationConstraint (regression: issue #11)
	euBucket := "go-sdk-test-bucket-eu"
	_, err = svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(euBucket),
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraintEuCentral1,
		},
	})
	check("S3 CreateBucket with LocationConstraint", err)

	// GetBucketLocation
	loc, err := svc.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(euBucket)})
	check("S3 GetBucketLocation", err, err == nil && string(loc.LocationConstraint) == "eu-central-1")

	lb, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	check("S3 ListBuckets", err, err == nil && len(lb.Buckets) > 0)

	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(content),
		ContentType: aws.String("application/json"),
	})
	check("S3 PutObject", err)

	out, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		var buf bytes.Buffer
		buf.ReadFrom(out.Body)
		out.Body.Close()
		check("S3 GetObject", nil, buf.String() == content)
	} else {
		check("S3 GetObject", err)
	}

	head, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	check("S3 HeadObject", err)
	check("S3 HeadObject LastModified second precision",
		err, err == nil && head.LastModified != nil && head.LastModified.Nanosecond() == 0)

	lo, err := svc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	check("S3 ListObjectsV2", err, err == nil && len(lo.Contents) > 0)

	_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + key),
		Key:        aws.String("copy-" + key),
	})
	check("S3 CopyObject", err)

	// CopyObject with non-ASCII (multibyte) key — regression: issue #93
	// The Go SDK does not URL-encode CopySource headers; encode each path segment manually.
	nonAsciiKey := "src/テスト画像.png"
	nonAsciiDst := "dst/テスト画像.png"
	encodedNonAsciiKey := strings.Join(func() []string {
		segs := strings.Split(nonAsciiKey, "/")
		for i, s := range segs {
			segs[i] = url.PathEscape(s)
		}
		return segs
	}(), "/")
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(nonAsciiKey),
		Body:   strings.NewReader("non-ascii content"),
	})
	if err == nil {
		_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			CopySource: aws.String(bucket + "/" + encodedNonAsciiKey),
			Key:        aws.String(nonAsciiDst),
		})
		check("S3 CopyObject non-ASCII key", err)
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(nonAsciiKey)})
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(nonAsciiDst)})
	} else {
		check("S3 CopyObject non-ASCII key", err)
	}

	// Large object upload (25 MB) — validates fix for upload size limit
	const largeKey = "large-object-25mb.bin"
	largeSizeBytes := int64(25 * 1024 * 1024)
	largePayload := make([]byte, largeSizeBytes)
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(largeKey),
		Body:          bytes.NewReader(largePayload),
		ContentLength: &largeSizeBytes,
		ContentType:   aws.String("application/octet-stream"),
	})
	check("S3 PutObject 25 MB", err)
	if err == nil {
		headLarge, headLargeErr := svc.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(largeKey),
		})
		check("S3 HeadObject 25 MB content-length",
			headLargeErr, headLargeErr == nil && headLarge.ContentLength != nil && *headLarge.ContentLength == largeSizeBytes)
		svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(largeKey)})
	}

	// cleanup
	svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy-" + key)})
	svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(euBucket)})
	_, err = svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	check("S3 DeleteBucket", err)
}

func runS3Cors(cfg aws.Config) {
	fmt.Println("--- S3 CORS Enforcement Tests ---")
	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	endpoint := os.Getenv("FLOCI_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	baseURL := endpoint
	bucket := fmt.Sprintf("go-cors-test-%d", time.Now().UnixMilli())
	objectKey := "cors-test.txt"

	// raw sends a raw HTTP request and returns the status code and response headers.
	raw := func(method, path string, headers map[string]string) (int, http.Header, error) {
		rawURL := baseURL + "/" + bucket + path
		req, err := http.NewRequest(method, rawURL, nil)
		if err != nil {
			return 0, nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, resp.Header, nil
	}

	// ── Setup ─────────────────────────────────────────────────────────────────
	_, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	check("S3 CORS setup CreateBucket", err)
	if err != nil {
		return
	}
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(objectKey),
		Body:        strings.NewReader("hello cors"),
		ContentType: aws.String("text/plain"),
	})
	check("S3 CORS setup PutObject", err)
	if err != nil {
		svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		return
	}

	// ── No CORS config: preflight → 403 ──────────────────────────────────────
	status, _, err := raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "http://localhost:3000",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS preflight without config → 403", err, err == nil && status == 403)

	// ── Wildcard-origin CORS config ───────────────────────────────────────────
	maxAge3000 := int32(3000)
	_, err = svc.PutBucketCors(ctx, &s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &s3types.CORSConfiguration{
			CORSRules: []s3types.CORSRule{
				{
					AllowedOrigins: []string{"*"},
					AllowedMethods: []string{"GET", "PUT", "POST", "DELETE", "HEAD"},
					AllowedHeaders: []string{"*"},
					ExposeHeaders:  []string{"ETag"},
					MaxAgeSeconds:  &maxAge3000,
				},
			},
		},
	})
	check("S3 CORS PutBucketCors (wildcard)", err)

	status, hdrs, err := raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "http://localhost:3000",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS wildcard preflight → 200", err, err == nil && status == 200)
	check("S3 CORS wildcard preflight → Allow-Origin: *", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "*")
	check("S3 CORS wildcard preflight → Max-Age: 3000", err,
		err == nil && hdrs.Get("Access-Control-Max-Age") == "3000")
	check("S3 CORS wildcard preflight → Allow-Methods contains GET", err,
		err == nil && strings.Contains(strings.ToUpper(hdrs.Get("Access-Control-Allow-Methods")), "GET"))

	// Actual GET with Origin → receives CORS response headers
	status, hdrs, err = raw("GET", "/"+objectKey, map[string]string{"Origin": "http://localhost:3000"})
	check("S3 CORS actual GET → Allow-Origin: *", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "*")
	varyHasOrigin := func(vary string) bool {
		for _, tok := range strings.Split(vary, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), "origin") {
				return true
			}
		}
		return false
	}
	check("S3 CORS actual GET → Vary: Origin", err,
		err == nil && varyHasOrigin(hdrs.Get("Vary")))
	check("S3 CORS actual GET → Expose-Headers contains ETag", err,
		err == nil && strings.Contains(hdrs.Get("Access-Control-Expose-Headers"), "ETag"))

	// Actual GET without Origin → no CORS headers
	_, hdrs, err = raw("GET", "/"+objectKey, nil)
	check("S3 CORS actual GET (no Origin) → no Allow-Origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "")

	// OPTIONS without Origin → no CORS headers
	_, hdrs, err = raw("OPTIONS", "/"+objectKey, nil)
	check("S3 CORS OPTIONS without Origin → no Allow-Origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "")

	// ── Specific-origin CORS config ───────────────────────────────────────────
	maxAge600 := int32(600)
	_, err = svc.PutBucketCors(ctx, &s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &s3types.CORSConfiguration{
			CORSRules: []s3types.CORSRule{
				{
					AllowedOrigins: []string{"https://example.com"},
					AllowedMethods: []string{"GET", "PUT"},
					AllowedHeaders: []string{"Content-Type", "Authorization"},
					ExposeHeaders:  []string{"ETag", "x-amz-request-id"},
					MaxAgeSeconds:  &maxAge600,
				},
			},
		},
	})
	check("S3 CORS PutBucketCors (specific origin)", err)

	status, hdrs, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                         "https://example.com",
		"Access-Control-Request-Method":  "GET",
		"Access-Control-Request-Headers": "Content-Type",
	})
	check("S3 CORS specific origin preflight → 200", err, err == nil && status == 200)
	check("S3 CORS specific origin preflight → echoes origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "https://example.com")
	check("S3 CORS specific origin preflight → Max-Age: 600", err,
		err == nil && hdrs.Get("Access-Control-Max-Age") == "600")

	status, _, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "https://attacker.evil.com",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS non-matching origin → 403", err, err == nil && status == 403)

	status, _, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "https://example.com",
		"Access-Control-Request-Method": "DELETE",
	})
	check("S3 CORS non-matching method → 403", err, err == nil && status == 403)

	_, hdrs, err = raw("GET", "/"+objectKey, map[string]string{"Origin": "https://example.com"})
	check("S3 CORS actual GET matching specific origin → echoes origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "https://example.com")

	_, hdrs, err = raw("GET", "/"+objectKey, map[string]string{"Origin": "https://not-allowed.com"})
	check("S3 CORS actual GET non-matching origin → no Allow-Origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "")

	// ── DeleteBucketCors → preflights return 403 again ────────────────────────
	_, err = svc.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
	check("S3 CORS DeleteBucketCors", err)

	status, _, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "http://localhost:3000",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS preflight after delete → 403", err, err == nil && status == 403)

	// ── Subdomain wildcard origin pattern ─────────────────────────────────────
	maxAge120 := int32(120)
	_, err = svc.PutBucketCors(ctx, &s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &s3types.CORSConfiguration{
			CORSRules: []s3types.CORSRule{
				{
					AllowedOrigins: []string{"http://*.example.com"},
					AllowedMethods: []string{"GET"},
					AllowedHeaders: []string{"*"},
					MaxAgeSeconds:  &maxAge120,
				},
			},
		},
	})
	check("S3 CORS PutBucketCors (subdomain wildcard)", err)

	status, hdrs, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "http://app.example.com",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS subdomain wildcard matches http://app.example.com → 200", err,
		err == nil && status == 200)
	check("S3 CORS subdomain wildcard echoes matched origin", err,
		err == nil && hdrs.Get("Access-Control-Allow-Origin") == "http://app.example.com")

	status, _, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "https://app.example.com",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS subdomain wildcard rejects https:// → 403", err, err == nil && status == 403)

	status, _, err = raw("OPTIONS", "/"+objectKey, map[string]string{
		"Origin":                        "http://app.other.com",
		"Access-Control-Request-Method": "GET",
	})
	check("S3 CORS subdomain wildcard rejects different domain → 403", err, err == nil && status == 403)

	// ── Cleanup ────────────────────────────────────────────────────────────────
	svc.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
	svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objectKey)})
	_, err = svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	check("S3 CORS cleanup DeleteBucket", err)
}

// ── DynamoDB ──────────────────────────────────────────────────────────────────

func runDynamoDB(cfg aws.Config) {
	fmt.Println("--- DynamoDB Tests ---")
	svc := dynamodb.NewFromConfig(cfg)
	table := "go-sdk-test-table"

	_, err := svc.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
	})
	check("DynamoDB CreateTable", err)
	if err != nil {
		return
	}

	dt, err := svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	check("DynamoDB DescribeTable", err, err == nil && dt.Table.TableStatus == ddbtypes.TableStatusActive)

	lt, err := svc.ListTables(ctx, &dynamodb.ListTablesInput{})
	check("DynamoDB ListTables", err, err == nil && len(lt.TableNames) > 0)

	_, err = svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "user#1"},
			"sk":   &ddbtypes.AttributeValueMemberS{Value: "profile"},
			"name": &ddbtypes.AttributeValueMemberS{Value: "Alice"},
			"age":  &ddbtypes.AttributeValueMemberN{Value: "30"},
		},
	})
	check("DynamoDB PutItem", err)

	gi, err := svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "user#1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "profile"},
		},
	})
	if err == nil {
		attr, _ := gi.Item["name"].(*ddbtypes.AttributeValueMemberS)
		check("DynamoDB GetItem", nil, attr != nil && attr.Value == "Alice")
	} else {
		check("DynamoDB GetItem", err)
	}

	_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "user#1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "profile"},
		},
		UpdateExpression:         aws.String("SET #n = :n"),
		ExpressionAttributeNames: map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":n": &ddbtypes.AttributeValueMemberS{Value: "Alice Updated"},
		},
	})
	check("DynamoDB UpdateItem", err)

	qr, err := svc.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: "user#1"},
		},
	})
	check("DynamoDB Query", err, err == nil && len(qr.Items) > 0)

	sr, err := svc.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table)})
	check("DynamoDB Scan", err, err == nil && len(sr.Items) > 0)

	_, err = svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "user#1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "profile"},
		},
	})
	check("DynamoDB DeleteItem", err)

	_, err = svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	check("DynamoDB DeleteTable", err)
}

// ── Lambda ────────────────────────────────────────────────────────────────────

func runLambda(cfg aws.Config) {
	fmt.Println("--- Lambda Tests ---")
	svc := lambda.NewFromConfig(cfg)
	funcName := "go-sdk-test-func"
	roleARN := "arn:aws:iam::000000000000:role/test-role"

	_, err := svc.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String(funcName),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String(roleARN),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: minimalZip()},
	})
	check("Lambda CreateFunction", err)
	if err != nil {
		return
	}

	gf, err := svc.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(funcName)})
	check("Lambda GetFunction", err, err == nil && aws.ToString(gf.Configuration.FunctionName) == funcName)

	lf, err := svc.ListFunctions(ctx, &lambda.ListFunctionsInput{})
	check("Lambda ListFunctions", err, err == nil && len(lf.Functions) > 0)

	payload, _ := json.Marshal(map[string]string{"name": "GoSDK"})
	iv, err := svc.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(funcName),
		Payload:      payload,
	})
	check("Lambda Invoke", err, err == nil && iv.StatusCode == 200 && iv.FunctionError == nil)

	_, err = svc.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String(funcName)})
	check("Lambda DeleteFunction", err)
}

// ── IAM ───────────────────────────────────────────────────────────────────────

func runIAM(cfg aws.Config) {
	fmt.Println("--- IAM Tests ---")
	svc := iam.NewFromConfig(cfg)
	roleName := "go-sdk-test-role"
	assumePolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	_, err := svc.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(assumePolicy),
	})
	check("IAM CreateRole", err)
	if err != nil {
		return
	}

	gr, err := svc.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
	check("IAM GetRole", err, err == nil && aws.ToString(gr.Role.RoleName) == roleName)

	lr, err := svc.ListRoles(ctx, &iam.ListRolesInput{})
	check("IAM ListRoles", err, err == nil && len(lr.Roles) > 0)

	_, err = svc.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(roleName)})
	check("IAM DeleteRole", err)
}

// ── STS ───────────────────────────────────────────────────────────────────────

func runSTS(cfg aws.Config) {
	fmt.Println("--- STS Tests ---")
	svc := sts.NewFromConfig(cfg)

	r, err := svc.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	check("STS GetCallerIdentity", err, err == nil && aws.ToString(r.Account) != "")
}

// ── Secrets Manager ───────────────────────────────────────────────────────────

func runSecretsManager(cfg aws.Config) {
	fmt.Println("--- Secrets Manager Tests ---")
	svc := secretsmanager.NewFromConfig(cfg)
	secretName := "go-sdk-test/creds"
	secretVal := `{"username":"admin","password":"s3cret"}`

	cs, err := svc.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(secretVal),
	})
	check("SecretsManager CreateSecret", err)
	if err != nil {
		return
	}
	secretID := aws.ToString(cs.ARN)

	gsv, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretID)})
	check("SecretsManager GetSecretValue", err, err == nil && aws.ToString(gsv.SecretString) == secretVal)

	_, err = svc.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(secretID)})
	check("SecretsManager DescribeSecret", err)

	ls, err := svc.ListSecrets(ctx, &secretsmanager.ListSecretsInput{})
	check("SecretsManager ListSecrets", err, err == nil && len(ls.SecretList) > 0)

	newVal := `{"username":"admin","password":"n3wsecret"}`
	_, err = svc.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretID),
		SecretString: aws.String(newVal),
	})
	check("SecretsManager PutSecretValue", err)

	_, err = svc.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(secretID),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	check("SecretsManager DeleteSecret", err)
}

// ── KMS ───────────────────────────────────────────────────────────────────────

func runKMS(cfg aws.Config) {
	fmt.Println("--- KMS Tests ---")
	svc := kms.NewFromConfig(cfg)

	ck, err := svc.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("go-sdk-test key"),
	})
	check("KMS CreateKey", err)
	if err != nil {
		return
	}
	keyID := aws.ToString(ck.KeyMetadata.KeyId)

	dk, err := svc.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyID)})
	check("KMS DescribeKey", err, err == nil && aws.ToString(dk.KeyMetadata.KeyId) == keyID)

	lk, err := svc.ListKeys(ctx, &kms.ListKeysInput{})
	check("KMS ListKeys", err, err == nil && len(lk.Keys) > 0)

	enc, err := svc.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("hello-go-sdk"),
	})
	check("KMS Encrypt", err, err == nil && len(enc.CiphertextBlob) > 0)

	if err == nil {
		dec, err := svc.Decrypt(ctx, &kms.DecryptInput{
			CiphertextBlob: enc.CiphertextBlob,
			KeyId:          aws.String(keyID),
		})
		check("KMS Decrypt", err, err == nil && string(dec.Plaintext) == "hello-go-sdk")
	}

	gdkr, err := svc.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: kmstypes.DataKeySpecAes256,
	})
	check("KMS GenerateDataKey", err, err == nil && len(gdkr.Plaintext) == 32)
}

// ── Kinesis ───────────────────────────────────────────────────────────────────

func runKinesis(cfg aws.Config) {
	fmt.Println("--- Kinesis Tests ---")
	svc := kinesis.NewFromConfig(cfg)
	streamName := "go-sdk-test-stream"

	_, err := svc.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	check("Kinesis CreateStream", err)
	if err != nil {
		return
	}

	ls, err := svc.ListStreams(ctx, &kinesis.ListStreamsInput{})
	check("Kinesis ListStreams", err, err == nil && len(ls.StreamNames) > 0)

	ds, err := svc.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	check("Kinesis DescribeStream", err, err == nil && ds.StreamDescription.StreamStatus == kinesistypes.StreamStatusActive)
	if err != nil {
		svc.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(streamName)})
		return
	}

	pr, err := svc.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		Data:         []byte(`{"event":"go-sdk-test"}`),
		PartitionKey: aws.String("pk1"),
	})
	check("Kinesis PutRecord", err, err == nil && aws.ToString(pr.ShardId) != "")

	shardID := aws.ToString(ds.StreamDescription.Shards[0].ShardId)
	gsi, err := svc.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	check("Kinesis GetShardIterator", err)

	if err == nil {
		gr, err := svc.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: gsi.ShardIterator,
			Limit:         aws.Int32(10),
		})
		check("Kinesis GetRecords", err, err == nil && len(gr.Records) > 0)
	}

	_, err = svc.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(streamName)})
	check("Kinesis DeleteStream", err)
}

// ── CloudWatch Metrics ────────────────────────────────────────────────────────

func runCloudWatch(cfg aws.Config) {
	fmt.Println("--- CloudWatch Metrics Tests ---")
	svc := cloudwatch.NewFromConfig(cfg)
	namespace := "GoSDKTest"

	_, err := svc.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(namespace),
		MetricData: []cwtypes.MetricDatum{
			{
				MetricName: aws.String("RequestCount"),
				Value:      aws.Float64(42),
				Unit:       cwtypes.StandardUnitCount,
				Timestamp:  aws.Time(time.Now()),
			},
		},
	})
	check("CloudWatch PutMetricData", err)

	lm, err := svc.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String(namespace)})
	check("CloudWatch ListMetrics", err, err == nil && len(lm.Metrics) > 0)

	now := time.Now()
	gms, err := svc.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String(namespace),
		MetricName: aws.String("RequestCount"),
		StartTime:  aws.Time(now.Add(-5 * time.Minute)),
		EndTime:    aws.Time(now.Add(time.Minute)),
		Period:     aws.Int32(60),
		Statistics: []cwtypes.Statistic{cwtypes.StatisticSum},
	})
	check("CloudWatch GetMetricStatistics", err, err == nil && len(gms.Datapoints) > 0)
}

// ── Main ──────────────────────────────────────────────────────────────────────

func resolveEnabled(args []string) map[string]bool {
	if len(args) == 0 {
		return nil
	}
	m := make(map[string]bool)
	for _, a := range args {
		for _, p := range strings.Split(a, ",") {
			if t := strings.TrimSpace(strings.ToLower(p)); t != "" {
				m[t] = true
			}
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// ── S3 Notification Filter ────────────────────────────────────────────────────

func runS3Notifications(cfg aws.Config) {
	fmt.Println("--- S3 Notification Filter Tests ---")

	s3Svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	sqsSvc := sqs.NewFromConfig(cfg)
	snsSvc := sns.NewFromConfig(cfg)

	queueName := "s3-notif-filter-queue"
	topicName := "s3-notif-filter-topic"
	bucketName := "s3-notif-filter-bucket"

	// Create SQS queue
	_, err := sqsSvc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	check("S3Notif CreateQueue", err)
	if err != nil {
		return
	}
	queueArn := "arn:aws:sqs:us-east-1:000000000000:" + queueName

	// Create SNS topic
	ct, err := snsSvc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	check("S3Notif CreateTopic", err)
	if err != nil {
		return
	}
	topicArn := aws.ToString(ct.TopicArn)

	// Create S3 bucket
	_, err = s3Svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	check("S3Notif CreateBucket", err)
	if err != nil {
		return
	}

	// PutBucketNotificationConfiguration
	_, err = s3Svc.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucketName),
		NotificationConfiguration: &s3types.NotificationConfiguration{
			QueueConfigurations: []s3types.QueueConfiguration{
				{
					Id:       aws.String("sqs-filtered"),
					QueueArn: aws.String(queueArn),
					Events:   []s3types.Event{s3types.EventS3ObjectCreated},
					Filter: &s3types.NotificationConfigurationFilter{
						Key: &s3types.S3KeyFilter{
							FilterRules: []s3types.FilterRule{
								{Name: s3types.FilterRuleNamePrefix, Value: aws.String("incoming/")},
								{Name: s3types.FilterRuleNameSuffix, Value: aws.String(".csv")},
							},
						},
					},
				},
			},
			TopicConfigurations: []s3types.TopicConfiguration{
				{
					Id:       aws.String("sns-filtered"),
					TopicArn: aws.String(topicArn),
					Events:   []s3types.Event{s3types.EventS3ObjectRemoved},
					Filter: &s3types.NotificationConfigurationFilter{
						Key: &s3types.S3KeyFilter{
							FilterRules: []s3types.FilterRule{
								{Name: s3types.FilterRuleNamePrefix, Value: aws.String("")},
								{Name: s3types.FilterRuleNameSuffix, Value: aws.String(".txt")},
							},
						},
					},
				},
			},
		},
	})
	check("S3Notif PutBucketNotificationConfiguration", err)
	if err != nil {
		return
	}

	// GetBucketNotificationConfiguration and assert
	gnc, err := s3Svc.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	check("S3Notif GetBucketNotificationConfiguration", err)
	if err != nil {
		return
	}

	// Assert queue config
	check("S3Notif QueueConfig present",
		nil,
		len(gnc.QueueConfigurations) > 0 && aws.ToString(gnc.QueueConfigurations[0].QueueArn) == queueArn,
	)
	check("S3Notif QueueConfig has 2 filter rules",
		nil,
		len(gnc.QueueConfigurations) > 0 &&
			gnc.QueueConfigurations[0].Filter != nil &&
			gnc.QueueConfigurations[0].Filter.Key != nil &&
			len(gnc.QueueConfigurations[0].Filter.Key.FilterRules) == 2,
	)

	// Assert topic config
	check("S3Notif TopicConfig present",
		nil,
		len(gnc.TopicConfigurations) > 0 && aws.ToString(gnc.TopicConfigurations[0].TopicArn) == topicArn,
	)
	check("S3Notif TopicConfig has 2 filter rules",
		nil,
		len(gnc.TopicConfigurations) > 0 &&
			gnc.TopicConfigurations[0].Filter != nil &&
			gnc.TopicConfigurations[0].Filter.Key != nil &&
			len(gnc.TopicConfigurations[0].Filter.Key.FilterRules) == 2,
	)

	// Cleanup
	s3Svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	sqsSvc.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String("http://localhost:4566/000000000000/" + queueName),
	})
	snsSvc.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(topicArn)})
}

func main() {
	endpoint := os.Getenv("FLOCI_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	fmt.Println("=== Floci SDK Test (Go AWS SDK v2) ===")
	fmt.Printf("Endpoint: %s\n\n", endpoint)

	cfg := newCfg(endpoint)

	groups := []struct {
		name string
		fn   func(aws.Config)
	}{
		{"ssm", runSSM},
		{"sqs", runSQS},
		{"sns", runSNS},
		{"s3", runS3},
		{"s3-cors", runS3Cors},
		{"dynamodb", runDynamoDB},
		{"lambda", runLambda},
		{"iam", runIAM},
		{"sts", runSTS},
		{"secretsmanager", runSecretsManager},
		{"kms", runKMS},
		{"kinesis", runKinesis},
		{"cloudwatch", runCloudWatch},
		{"s3-notifications", runS3Notifications},
	}

	enabled := resolveEnabled(os.Args[1:])
	for _, g := range groups {
		if enabled == nil || enabled[g.name] {
			g.fn(cfg)
			fmt.Println()
		}
	}

	fmt.Printf("=== Results: %d passed, %d failed ===\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
