// Floci SDK Test — Rust (AWS SDK for Rust)
//
// Runs against the Floci AWS emulator. Configure via:
//
//   FLOCI_ENDPOINT=http://localhost:4566 (default)
//
// To run specific groups:
//
//   ./floci-test ssm sqs s3

use std::collections::HashSet;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3Builder;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, ObjectIdentifier,
};

static PASSED: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);

fn check(name: &str, result: Result<(), String>) {
    match result {
        Ok(()) => {
            PASSED.fetch_add(1, Ordering::Relaxed);
            println!("  PASS  {}", name);
        }
        Err(msg) => {
            FAILED.fetch_add(1, Ordering::Relaxed);
            println!("  FAIL  {} — {}", name, msg);
        }
    }
}

fn ok() -> Result<(), String> {
    Ok(())
}

fn err<E: std::fmt::Display>(e: E) -> Result<(), String> {
    Err(e.to_string())
}

async fn base_config(endpoint: &str) -> aws_config::SdkConfig {
    let creds = Credentials::new("test", "test", None, None, "static");
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_types::region::Region::new("us-east-1"))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .load()
        .await
}

// ── S3 ────────────────────────────────────────────────────────────────────────

async fn run_s3(endpoint: &str) {
    println!("--- S3 Tests ---");

    let base = base_config(endpoint).await;
    let s3 = aws_sdk_s3::Client::from_conf(S3Builder::from(&base).force_path_style(true).build());

    let bucket = "rust-sdk-test-bucket";
    let key = "test-object.json";
    let content = r#"{"source":"rust-sdk-test"}"#;

    // CreateBucket — default (no LocationConstraint)
    match s3.create_bucket().bucket(bucket).send().await {
        Ok(_) => check("S3 CreateBucket", ok()),
        Err(e) => {
            check("S3 CreateBucket", err(e));
            return;
        }
    }

    // CreateBucket — with LocationConstraint (regression: issue #11)
    let eu_bucket = "rust-sdk-test-bucket-eu";
    match s3
        .create_bucket()
        .bucket(eu_bucket)
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::EuCentral1)
                .build(),
        )
        .send()
        .await
    {
        Ok(_) => check("S3 CreateBucket with LocationConstraint", ok()),
        Err(e) => check("S3 CreateBucket with LocationConstraint", err(e)),
    }

    // ListBuckets
    match s3.list_buckets().send().await {
        Ok(r) => check(
            "S3 ListBuckets",
            if r.buckets().len() > 0 {
                ok()
            } else {
                Err("no buckets listed".into())
            },
        ),
        Err(e) => check("S3 ListBuckets", err(e)),
    }

    // PutObject
    match s3
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(bytes::Bytes::from(content).into())
        .content_type("application/json")
        .send()
        .await
    {
        Ok(_) => check("S3 PutObject", ok()),
        Err(e) => check("S3 PutObject", err(e)),
    }

    // GetObject
    match s3.get_object().bucket(bucket).key(key).send().await {
        Ok(r) => {
            let body = r.body.collect().await.map(|b| b.into_bytes());
            match body {
                Ok(b) => check(
                    "S3 GetObject",
                    if b.as_ref() == content.as_bytes() {
                        ok()
                    } else {
                        Err(format!(
                            "content mismatch: {:?}",
                            String::from_utf8_lossy(&b)
                        ))
                    },
                ),
                Err(e) => check("S3 GetObject", Err(e.to_string())),
            }
        }
        Err(e) => check("S3 GetObject", err(e)),
    }

    // HeadObject
    match s3.head_object().bucket(bucket).key(key).send().await {
        Ok(r) => {
            check("S3 HeadObject", ok());
            check(
                "S3 HeadObject LastModified second precision",
                match r.last_modified() {
                    Some(t) => {
                        if t.subsec_nanos() == 0 {
                            ok()
                        } else {
                            Err(format!("sub-second nanos: {}", t.subsec_nanos()))
                        }
                    }
                    None => Err("no last_modified".to_string()),
                },
            );
        }
        Err(e) => check("S3 HeadObject", err(e)),
    }

    // ListObjectsV2
    match s3.list_objects_v2().bucket(bucket).send().await {
        Ok(r) => check(
            "S3 ListObjectsV2",
            if r.contents().len() > 0 {
                ok()
            } else {
                Err("no objects listed".into())
            },
        ),
        Err(e) => check("S3 ListObjectsV2", err(e)),
    }

    // CopyObject
    let copy_source = format!("{}/{}", bucket, key);
    let copy_key = format!("copy-{}", key);
    match s3
        .copy_object()
        .bucket(bucket)
        .copy_source(&copy_source)
        .key(&copy_key)
        .send()
        .await
    {
        Ok(_) => check("S3 CopyObject", ok()),
        Err(e) => check("S3 CopyObject", err(e)),
    }

    // GetBucketLocation — with LocationConstraint bucket
    match s3.get_bucket_location().bucket(eu_bucket).send().await {
        Ok(r) => check(
            "S3 GetBucketLocation",
            if r.location_constraint()
                .map(|c| c.as_str() == "eu-central-1")
                .unwrap_or(false)
            {
                ok()
            } else {
                Err(format!(
                    "unexpected location: {:?}",
                    r.location_constraint()
                ))
            },
        ),
        Err(e) => check("S3 GetBucketLocation", err(e)),
    }

    // Large object upload (25 MB) — validates fix for upload size limit
    let large_key = "large-object-25mb.bin";
    let large_size: i64 = 25 * 1024 * 1024;
    let large_payload = vec![0u8; large_size as usize];
    match s3
        .put_object()
        .bucket(bucket)
        .key(large_key)
        .body(bytes::Bytes::from(large_payload).into())
        .content_type("application/octet-stream")
        .content_length(large_size)
        .send()
        .await
    {
        Ok(_) => {
            check("S3 PutObject 25 MB", ok());
            match s3.head_object().bucket(bucket).key(large_key).send().await {
                Ok(r) => check(
                    "S3 HeadObject 25 MB content-length",
                    if r.content_length() == Some(large_size) {
                        ok()
                    } else {
                        Err(format!(
                            "expected {} bytes, got {:?}",
                            large_size,
                            r.content_length()
                        ))
                    },
                ),
                Err(e) => check("S3 HeadObject 25 MB content-length", err(e)),
            }
            let _ = s3
                .delete_object()
                .bucket(bucket)
                .key(large_key)
                .send()
                .await;
        }
        Err(e) => check("S3 PutObject 25 MB", err(e)),
    }

    // cleanup
    let _ = s3
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key(key).build().unwrap())
                .objects(ObjectIdentifier::builder().key(copy_key).build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await;
    let _ = s3.delete_bucket().bucket(eu_bucket).send().await;
    match s3.delete_bucket().bucket(bucket).send().await {
        Ok(_) => check("S3 DeleteBucket", ok()),
        Err(e) => check("S3 DeleteBucket", err(e)),
    }
}

// ── SSM ───────────────────────────────────────────────────────────────────────

async fn run_ssm(endpoint: &str) {
    println!("--- SSM Tests ---");

    let base = base_config(endpoint).await;
    let ssm = aws_sdk_ssm::Client::new(&base);

    let name = "/rust-sdk-test/param";
    let value = "rust-sdk-value";

    match ssm
        .put_parameter()
        .name(name)
        .value(value)
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .overwrite(true)
        .send()
        .await
    {
        Ok(_) => check("SSM PutParameter", ok()),
        Err(e) => check("SSM PutParameter", err(e)),
    }

    match ssm.get_parameter().name(name).send().await {
        Ok(r) => check(
            "SSM GetParameter",
            if r.parameter().and_then(|p| p.value()).unwrap_or("") == value {
                ok()
            } else {
                Err("value mismatch".into())
            },
        ),
        Err(e) => check("SSM GetParameter", err(e)),
    }

    match ssm.get_parameters().names(name).send().await {
        Ok(r) => check(
            "SSM GetParameters",
            if r.parameters().len() == 1 {
                ok()
            } else {
                Err("expected 1 parameter".into())
            },
        ),
        Err(e) => check("SSM GetParameters", err(e)),
    }

    match ssm.describe_parameters().send().await {
        Ok(r) => check(
            "SSM DescribeParameters",
            if !r.parameters().is_empty() {
                ok()
            } else {
                Err("no parameters".into())
            },
        ),
        Err(e) => check("SSM DescribeParameters", err(e)),
    }

    match ssm
        .get_parameters_by_path()
        .path("/rust-sdk-test")
        .send()
        .await
    {
        Ok(r) => check(
            "SSM GetParametersByPath",
            if !r.parameters().is_empty() {
                ok()
            } else {
                Err("no parameters".into())
            },
        ),
        Err(e) => check("SSM GetParametersByPath", err(e)),
    }

    match ssm.delete_parameter().name(name).send().await {
        Ok(_) => check("SSM DeleteParameter", ok()),
        Err(e) => check("SSM DeleteParameter", err(e)),
    }
}

// ── SQS ───────────────────────────────────────────────────────────────────────

async fn run_sqs(endpoint: &str) {
    println!("--- SQS Tests ---");

    let base = base_config(endpoint).await;
    let sqs = aws_sdk_sqs::Client::new(&base);

    let queue = "rust-sdk-test-queue";

    let url = match sqs.create_queue().queue_name(queue).send().await {
        Ok(r) => {
            check("SQS CreateQueue", ok());
            r.queue_url().unwrap_or("").to_string()
        }
        Err(e) => {
            check("SQS CreateQueue", err(e));
            return;
        }
    };

    match sqs.list_queues().send().await {
        Ok(r) => check(
            "SQS ListQueues",
            if !r.queue_urls().is_empty() {
                ok()
            } else {
                Err("no queues".into())
            },
        ),
        Err(e) => check("SQS ListQueues", err(e)),
    }

    let receipt = match sqs
        .send_message()
        .queue_url(&url)
        .message_body("hello from rust")
        .send()
        .await
    {
        Ok(_) => {
            check("SQS SendMessage", ok());
            // receive
            match sqs
                .receive_message()
                .queue_url(&url)
                .max_number_of_messages(1)
                .send()
                .await
            {
                Ok(r) => {
                    let msgs = r.messages();
                    check(
                        "SQS ReceiveMessage",
                        if msgs.len() == 1 {
                            ok()
                        } else {
                            Err("expected 1 message".into())
                        },
                    );
                    msgs.first()
                        .and_then(|m| m.receipt_handle())
                        .map(|s| s.to_string())
                }
                Err(e) => {
                    check("SQS ReceiveMessage", err(e));
                    None
                }
            }
        }
        Err(e) => {
            check("SQS SendMessage", err(e));
            None
        }
    };

    if let Some(handle) = receipt {
        match sqs
            .delete_message()
            .queue_url(&url)
            .receipt_handle(handle)
            .send()
            .await
        {
            Ok(_) => check("SQS DeleteMessage", ok()),
            Err(e) => check("SQS DeleteMessage", err(e)),
        }
    }

    match sqs.delete_queue().queue_url(&url).send().await {
        Ok(_) => check("SQS DeleteQueue", ok()),
        Err(e) => check("SQS DeleteQueue", err(e)),
    }
}

// ── STS ───────────────────────────────────────────────────────────────────────

async fn run_sts(endpoint: &str) {
    println!("--- STS Tests ---");

    let base = base_config(endpoint).await;
    let sts = aws_sdk_sts::Client::new(&base);

    match sts.get_caller_identity().send().await {
        Ok(r) => check(
            "STS GetCallerIdentity",
            if r.account().is_some() {
                ok()
            } else {
                Err("no account id".into())
            },
        ),
        Err(e) => check("STS GetCallerIdentity", err(e)),
    }
}

// ── KMS ───────────────────────────────────────────────────────────────────────

async fn run_kms(endpoint: &str) {
    println!("--- KMS Tests ---");

    let base = base_config(endpoint).await;
    let kms = aws_sdk_kms::Client::new(&base);

    let key_id = match kms
        .create_key()
        .description("rust-sdk-test-key")
        .send()
        .await
    {
        Ok(r) => {
            check("KMS CreateKey", ok());
            r.key_metadata()
                .map(|m| m.key_id().to_string())
                .unwrap_or_default()
        }
        Err(e) => {
            check("KMS CreateKey", err(e));
            return;
        }
    };

    match kms.list_keys().send().await {
        Ok(r) => check(
            "KMS ListKeys",
            if !r.keys().is_empty() {
                ok()
            } else {
                Err("no keys".into())
            },
        ),
        Err(e) => check("KMS ListKeys", err(e)),
    }

    match kms
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_smithy_types::Blob::new("rust-kms-test"))
        .send()
        .await
    {
        Ok(r) => {
            let cipher = r
                .ciphertext_blob()
                .cloned()
                .unwrap_or_else(|| aws_smithy_types::Blob::new([]));
            check(
                "KMS Encrypt",
                if !cipher.as_ref().is_empty() {
                    ok()
                } else {
                    Err("empty ciphertext".into())
                },
            );

            match kms.decrypt().ciphertext_blob(cipher).send().await {
                Ok(d) => {
                    let plain = d
                        .plaintext()
                        .cloned()
                        .unwrap_or_else(|| aws_smithy_types::Blob::new([]));
                    check(
                        "KMS Decrypt",
                        if plain.as_ref() == b"rust-kms-test" {
                            ok()
                        } else {
                            Err("plaintext mismatch".into())
                        },
                    )
                }
                Err(e) => check("KMS Decrypt", err(e)),
            }
        }
        Err(e) => check("KMS Encrypt", err(e)),
    }

    // schedule deletion (min 7 days)
    let _ = kms
        .schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await;
}

// ── Secrets Manager ───────────────────────────────────────────────────────────

async fn run_secrets_manager(endpoint: &str) {
    println!("--- Secrets Manager Tests ---");

    let base = base_config(endpoint).await;
    let sm = aws_sdk_secretsmanager::Client::new(&base);

    let secret_name = "rust-sdk-test-secret";

    match sm
        .create_secret()
        .name(secret_name)
        .secret_string(r#"{"key":"rust-value"}"#)
        .send()
        .await
    {
        Ok(_) => check("SecretsManager CreateSecret", ok()),
        Err(e) => check("SecretsManager CreateSecret", err(e)),
    }

    match sm.get_secret_value().secret_id(secret_name).send().await {
        Ok(r) => check(
            "SecretsManager GetSecretValue",
            if r.secret_string().is_some() {
                ok()
            } else {
                Err("no secret string".into())
            },
        ),
        Err(e) => check("SecretsManager GetSecretValue", err(e)),
    }

    match sm.list_secrets().send().await {
        Ok(r) => check(
            "SecretsManager ListSecrets",
            if !r.secret_list().is_empty() {
                ok()
            } else {
                Err("no secrets".into())
            },
        ),
        Err(e) => check("SecretsManager ListSecrets", err(e)),
    }

    let _ = sm
        .delete_secret()
        .secret_id(secret_name)
        .force_delete_without_recovery(true)
        .send()
        .await;
}

// ── entry point ───────────────────────────────────────────────────────────────

fn resolve_enabled(args: &[String]) -> Option<HashSet<String>> {
    let mut groups: HashSet<String> = args
        .iter()
        .flat_map(|a| a.split(','))
        .map(|s| s.to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    if let Ok(env_val) = env::var("FLOCI_TESTS") {
        for t in env_val.split(',') {
            let t = t.trim().to_lowercase();
            if !t.is_empty() {
                groups.insert(t);
            }
        }
    }

    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

#[tokio::main]
async fn main() {
    let endpoint = env::var("FLOCI_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".into());

    println!("=== Floci SDK Test (Rust AWS SDK) ===");
    println!("Endpoint: {}\n", endpoint);

    let args: Vec<String> = env::args().skip(1).collect();
    let enabled = resolve_enabled(&args);

    let groups: Vec<(
        &str,
        fn(&str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
    )> = vec![
        ("ssm", |ep| Box::pin(run_ssm(ep))),
        ("sqs", |ep| Box::pin(run_sqs(ep))),
        ("s3", |ep| Box::pin(run_s3(ep))),
        ("sts", |ep| Box::pin(run_sts(ep))),
        ("kms", |ep| Box::pin(run_kms(ep))),
        ("secretsmanager", |ep| Box::pin(run_secrets_manager(ep))),
    ];

    for (name, run) in &groups {
        if enabled.as_ref().map(|e| e.contains(*name)).unwrap_or(true) {
            run(&endpoint).await;
            println!();
        }
    }

    let passed = PASSED.load(Ordering::Relaxed);
    let failed = FAILED.load(Ordering::Relaxed);
    println!("=== Results: {} passed, {} failed ===", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }
}
