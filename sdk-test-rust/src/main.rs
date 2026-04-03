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
    BucketLocationConstraint, CorsConfiguration, CorsRule, CreateBucketConfiguration, Delete,
    ObjectIdentifier,
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

// Percent-encodes non-ASCII bytes in an S3 key, preserving / as a path separator.
// The Rust SDK does not URL-encode CopySource headers, so this must be done manually.
fn s3_encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
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

    // CopyObject with non-ASCII (multibyte) key — regression: issue #93
    // The Rust SDK does not URL-encode CopySource headers; encode the key manually.
    let non_ascii_key = "src/テスト画像.png";
    let non_ascii_dst = "dst/テスト画像.png";
    match s3
        .put_object()
        .bucket(bucket)
        .key(non_ascii_key)
        .body(bytes::Bytes::from("non-ascii content").into())
        .send()
        .await
    {
        Ok(_) => {
            let non_ascii_copy_source = format!("{}/{}", bucket, s3_encode_key(non_ascii_key));
            match s3
                .copy_object()
                .bucket(bucket)
                .copy_source(&non_ascii_copy_source)
                .key(non_ascii_dst)
                .send()
                .await
            {
                Ok(_) => check("S3 CopyObject non-ASCII key", ok()),
                Err(e) => check("S3 CopyObject non-ASCII key", err(e)),
            }
            let _ = s3
                .delete_object()
                .bucket(bucket)
                .key(non_ascii_key)
                .send()
                .await;
            let _ = s3
                .delete_object()
                .bucket(bucket)
                .key(non_ascii_dst)
                .send()
                .await;
        }
        Err(e) => check("S3 CopyObject non-ASCII key", err(e)),
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

// ── S3 CORS Enforcement ───────────────────────────────────────────────────────

async fn run_s3_cors(endpoint: &str) {
    println!("--- S3 CORS Enforcement Tests ---");

    let base = base_config(endpoint).await;
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&base)
            .force_path_style(true)
            .build(),
    );

    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let bucket = format!("rust-cors-{}", millis);
    let object_key = "cors-test.txt";
    let object_url = format!("{}/{}/{}", endpoint, bucket, object_key);

    // reqwest::Client for raw HTTP — bypasses the AWS SDK so CORS headers are
    // tested exactly as a browser would receive them.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("reqwest client");

    /// Sends a raw HTTP request and returns `(status_code, response_headers)`.
    async fn raw_req(
        http: &reqwest::Client,
        method: &str,
        url: &str,
        hdrs: &[(&str, &str)],
    ) -> Result<(u16, reqwest::header::HeaderMap), String> {
        let m = match method {
            "GET" => reqwest::Method::GET,
            "PUT" => reqwest::Method::PUT,
            "POST" => reqwest::Method::POST,
            "DELETE" => reqwest::Method::DELETE,
            "OPTIONS" => reqwest::Method::OPTIONS,
            "HEAD" => reqwest::Method::HEAD,
            other => return Err(format!("unsupported method: {}", other)),
        };
        let mut builder = http.request(m, url);
        for (k, v) in hdrs {
            builder = builder.header(*k, *v);
        }
        let resp = builder.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        Ok((status, headers))
    }

    /// Returns the value of a response header as `&str`, or `""` if absent / non-ASCII.
    macro_rules! hdr {
        ($map:expr, $name:expr) => {
            $map.get($name).and_then(|v| v.to_str().ok()).unwrap_or("")
        };
    }

    // ── Setup: create bucket + upload test object ─────────────────────────────
    match s3.create_bucket().bucket(&bucket).send().await {
        Ok(_) => check("S3 CORS: setup CreateBucket", ok()),
        Err(e) => {
            check("S3 CORS: setup CreateBucket", err(e));
            return;
        }
    }
    match s3
        .put_object()
        .bucket(&bucket)
        .key(object_key)
        .body(bytes::Bytes::from("hello cors").into())
        .content_type("text/plain")
        .send()
        .await
    {
        Ok(_) => check("S3 CORS: setup PutObject", ok()),
        Err(e) => {
            check("S3 CORS: setup PutObject", err(e));
            let _ = s3.delete_bucket().bucket(&bucket).send().await;
            return;
        }
    }

    // ── No CORS config: OPTIONS preflight → 403 ──────────────────────────────
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "http://localhost:3000"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: no config preflight → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check("S3 CORS: no config preflight → 403", Err(e)),
    }

    // ── Wildcard-origin CORS config ───────────────────────────────────────────
    let wildcard_cors = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_origins("*")
                .allowed_methods("GET")
                .allowed_methods("PUT")
                .allowed_methods("POST")
                .allowed_methods("DELETE")
                .allowed_methods("HEAD")
                .allowed_headers("*")
                .expose_headers("ETag")
                .max_age_seconds(3000)
                .build()
                .expect("valid CorsRule"),
        )
        .build()
        .expect("valid CorsConfiguration");

    match s3
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(wildcard_cors)
        .send()
        .await
    {
        Ok(_) => check("S3 CORS: PutBucketCors wildcard", ok()),
        Err(e) => check("S3 CORS: PutBucketCors wildcard", err(e)),
    }

    // Wildcard preflight → 200 with correct CORS response headers
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "http://localhost:3000"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, h)) => {
            check(
                "S3 CORS: wildcard preflight → 200",
                if s == 200 {
                    ok()
                } else {
                    Err(format!("got {}", s))
                },
            );
            check(
                "S3 CORS: wildcard preflight Allow-Origin: *",
                if hdr!(h, "access-control-allow-origin") == "*" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-allow-origin")))
                },
            );
            check(
                "S3 CORS: wildcard preflight Max-Age: 3000",
                if hdr!(h, "access-control-max-age") == "3000" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-max-age")))
                },
            );
            check(
                "S3 CORS: wildcard preflight Allow-Methods contains GET",
                if hdr!(h, "access-control-allow-methods")
                    .to_uppercase()
                    .contains("GET")
                {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-allow-methods")))
                },
            );
        }
        Err(e) => {
            let e: Result<(), String> = Err(e);
            check("S3 CORS: wildcard preflight → 200", e.clone());
            check("S3 CORS: wildcard preflight Allow-Origin: *", e.clone());
            check("S3 CORS: wildcard preflight Max-Age: 3000", e.clone());
            check("S3 CORS: wildcard preflight Allow-Methods contains GET", e);
        }
    }

    // Actual GET with Origin → Access-Control-Allow-Origin: * + Vary: Origin
    match raw_req(
        &http,
        "GET",
        &object_url,
        &[("Origin", "http://localhost:3000")],
    )
    .await
    {
        Ok((s, h)) => {
            check(
                "S3 CORS: GET with Origin → 200",
                if s == 200 {
                    ok()
                } else {
                    Err(format!("got {}", s))
                },
            );
            check(
                "S3 CORS: GET with Origin → Allow-Origin: *",
                if hdr!(h, "access-control-allow-origin") == "*" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-allow-origin")))
                },
            );
            let vary_has_origin = hdr!(h, "vary")
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("origin"));
            check(
                "S3 CORS: GET with Origin → Vary: Origin",
                if vary_has_origin {
                    ok()
                } else {
                    Err("Vary header missing 'Origin'".into())
                },
            );
            check(
                "S3 CORS: GET with Origin → Expose-Headers contains ETag",
                if hdr!(h, "access-control-expose-headers").contains("ETag") {
                    ok()
                } else {
                    Err("ETag missing from Expose-Headers".into())
                },
            );
        }
        Err(e) => {
            let e: Result<(), String> = Err(e);
            check("S3 CORS: GET with Origin → 200", e.clone());
            check("S3 CORS: GET with Origin → Allow-Origin: *", e.clone());
            check("S3 CORS: GET with Origin → Vary: Origin", e.clone());
            check("S3 CORS: GET with Origin → Expose-Headers contains ETag", e);
        }
    }

    // Actual GET without Origin → no CORS headers
    match raw_req(&http, "GET", &object_url, &[]).await {
        Ok((_, h)) => check(
            "S3 CORS: GET without Origin → no Allow-Origin",
            if h.get("access-control-allow-origin").is_none() {
                ok()
            } else {
                Err("unexpected Access-Control-Allow-Origin".into())
            },
        ),
        Err(e) => check("S3 CORS: GET without Origin → no Allow-Origin", Err(e)),
    }

    // OPTIONS without Origin → no CORS headers
    match raw_req(&http, "OPTIONS", &object_url, &[]).await {
        Ok((_, h)) => check(
            "S3 CORS: OPTIONS without Origin → no Allow-Origin",
            if h.get("access-control-allow-origin").is_none() {
                ok()
            } else {
                Err("unexpected Access-Control-Allow-Origin".into())
            },
        ),
        Err(e) => check("S3 CORS: OPTIONS without Origin → no Allow-Origin", Err(e)),
    }

    // ── Specific-origin CORS config ───────────────────────────────────────────
    let specific_cors = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_origins("https://example.com")
                .allowed_methods("GET")
                .allowed_methods("PUT")
                .allowed_headers("Content-Type")
                .allowed_headers("Authorization")
                .expose_headers("ETag")
                .expose_headers("x-amz-request-id")
                .max_age_seconds(600)
                .build()
                .expect("valid CorsRule"),
        )
        .build()
        .expect("valid CorsConfiguration");

    match s3
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(specific_cors)
        .send()
        .await
    {
        Ok(_) => check("S3 CORS: PutBucketCors specific origin", ok()),
        Err(e) => check("S3 CORS: PutBucketCors specific origin", err(e)),
    }

    // Specific origin preflight matching → 200, echoes exact origin
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "https://example.com"),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "Content-Type"),
        ],
    )
    .await
    {
        Ok((s, h)) => {
            check(
                "S3 CORS: specific origin preflight → 200",
                if s == 200 {
                    ok()
                } else {
                    Err(format!("got {}", s))
                },
            );
            check(
                "S3 CORS: specific origin preflight echoes origin",
                if hdr!(h, "access-control-allow-origin") == "https://example.com" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-allow-origin")))
                },
            );
            check(
                "S3 CORS: specific origin preflight Max-Age: 600",
                if hdr!(h, "access-control-max-age") == "600" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-max-age")))
                },
            );
        }
        Err(e) => {
            let e: Result<(), String> = Err(e);
            check("S3 CORS: specific origin preflight → 200", e.clone());
            check(
                "S3 CORS: specific origin preflight echoes origin",
                e.clone(),
            );
            check("S3 CORS: specific origin preflight Max-Age: 600", e);
        }
    }

    // Non-matching origin → 403
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "https://attacker.evil.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: non-matching origin → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check("S3 CORS: non-matching origin → 403", Err(e)),
    }

    // Non-matching method → 403
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "https://example.com"),
            ("Access-Control-Request-Method", "DELETE"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: non-matching method → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check("S3 CORS: non-matching method → 403", Err(e)),
    }

    // GET with matching specific origin → echoes origin
    match raw_req(
        &http,
        "GET",
        &object_url,
        &[("Origin", "https://example.com")],
    )
    .await
    {
        Ok((_, h)) => check(
            "S3 CORS: GET matching specific origin → echoes origin",
            if hdr!(h, "access-control-allow-origin") == "https://example.com" {
                ok()
            } else {
                Err(format!("got: {:?}", h.get("access-control-allow-origin")))
            },
        ),
        Err(e) => check(
            "S3 CORS: GET matching specific origin → echoes origin",
            Err(e),
        ),
    }

    // GET with non-matching origin → no CORS headers
    match raw_req(
        &http,
        "GET",
        &object_url,
        &[("Origin", "https://not-allowed.com")],
    )
    .await
    {
        Ok((_, h)) => check(
            "S3 CORS: GET non-matching origin → no Allow-Origin",
            if h.get("access-control-allow-origin").is_none() {
                ok()
            } else {
                Err("unexpected Access-Control-Allow-Origin".into())
            },
        ),
        Err(e) => check("S3 CORS: GET non-matching origin → no Allow-Origin", Err(e)),
    }

    // ── DeleteBucketCors → preflights return 403 again ───────────────────────
    match s3.delete_bucket_cors().bucket(&bucket).send().await {
        Ok(_) => check("S3 CORS: DeleteBucketCors", ok()),
        Err(e) => check("S3 CORS: DeleteBucketCors", err(e)),
    }
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "http://localhost:3000"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: after delete preflight → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check("S3 CORS: after delete preflight → 403", Err(e)),
    }

    // ── Subdomain wildcard origin pattern ─────────────────────────────────────
    let subdomain_cors = CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_origins("http://*.example.com")
                .allowed_methods("GET")
                .allowed_headers("*")
                .max_age_seconds(120)
                .build()
                .expect("valid CorsRule"),
        )
        .build()
        .expect("valid CorsConfiguration");

    match s3
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(subdomain_cors)
        .send()
        .await
    {
        Ok(_) => check("S3 CORS: PutBucketCors subdomain wildcard", ok()),
        Err(e) => check("S3 CORS: PutBucketCors subdomain wildcard", err(e)),
    }

    // Subdomain wildcard matches http://app.example.com → 200, echoes origin
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "http://app.example.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, h)) => {
            check(
                "S3 CORS: subdomain wildcard matches http://app.example.com → 200",
                if s == 200 {
                    ok()
                } else {
                    Err(format!("got {}", s))
                },
            );
            check(
                "S3 CORS: subdomain wildcard echoes http://app.example.com",
                if hdr!(h, "access-control-allow-origin") == "http://app.example.com" {
                    ok()
                } else {
                    Err(format!("got: {:?}", h.get("access-control-allow-origin")))
                },
            );
        }
        Err(e) => {
            let e: Result<(), String> = Err(e);
            check(
                "S3 CORS: subdomain wildcard matches http://app.example.com → 200",
                e.clone(),
            );
            check(
                "S3 CORS: subdomain wildcard echoes http://app.example.com",
                e,
            );
        }
    }

    // Subdomain wildcard rejects https://app.example.com (scheme mismatch) → 403
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "https://app.example.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: subdomain wildcard rejects https:// → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check("S3 CORS: subdomain wildcard rejects https:// → 403", Err(e)),
    }

    // Subdomain wildcard rejects http://app.other.com (domain mismatch) → 403
    match raw_req(
        &http,
        "OPTIONS",
        &object_url,
        &[
            ("Origin", "http://app.other.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await
    {
        Ok((s, _)) => check(
            "S3 CORS: subdomain wildcard rejects different domain → 403",
            if s == 403 {
                ok()
            } else {
                Err(format!("expected 403, got {}", s))
            },
        ),
        Err(e) => check(
            "S3 CORS: subdomain wildcard rejects different domain → 403",
            Err(e),
        ),
    }

    // ── Cleanup ────────────────────────────────────────────────────────────────
    let _ = s3.delete_bucket_cors().bucket(&bucket).send().await;
    let _ = s3
        .delete_object()
        .bucket(&bucket)
        .key(object_key)
        .send()
        .await;
    match s3.delete_bucket().bucket(&bucket).send().await {
        Ok(_) => check("S3 CORS: cleanup DeleteBucket", ok()),
        Err(e) => check("S3 CORS: cleanup DeleteBucket", err(e)),
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

// ── S3 Notifications ─────────────────────────────────────────────────────────

async fn run_s3_notifications(endpoint: &str) {
    println!("--- S3 Notification Filter Tests ---");

    let base = base_config(endpoint).await;
    let s3 = aws_sdk_s3::Client::from_conf(
        S3Builder::from(&base).force_path_style(true).build(),
    );
    let sqs = aws_sdk_sqs::Client::new(&base);
    let sns = aws_sdk_sns::Client::new(&base);

    let bucket = "s3-notif-filter-bucket";
    let queue_name = "s3-notif-filter-queue";
    let topic_name = "s3-notif-filter-topic";
    let account_id = "000000000000";
    let queue_arn = format!("arn:aws:sqs:us-east-1:{}:{}", account_id, queue_name);

    // Setup
    let _ = sqs.create_queue().queue_name(queue_name).send().await;
    let topic_arn = match sns.create_topic().name(topic_name).send().await {
        Ok(r) => r.topic_arn.unwrap_or_default(),
        Err(e) => {
            check("S3 Notifications setup: CreateTopic", err(e));
            return;
        }
    };
    let _ = s3.create_bucket().bucket(bucket).send().await;

    // PutBucketNotificationConfiguration with filters
    use aws_sdk_s3::types::{
        Event as S3Event, FilterRule as S3FilterRule, FilterRuleName,
        NotificationConfigurationFilter, QueueConfiguration as S3QueueConfig,
        S3KeyFilter, TopicConfiguration as S3TopicConfig,
    };

    let queue_config = S3QueueConfig::builder()
        .id("sqs-filtered")
        .queue_arn(&queue_arn)
        .events(S3Event::S3ObjectCreated)
        .filter(
            NotificationConfigurationFilter::builder()
                .key(
                    S3KeyFilter::builder()
                        .filter_rules(
                            S3FilterRule::builder()
                                .name(FilterRuleName::Prefix)
                                .value("incoming/")
                                .build(),
                        )
                        .filter_rules(
                            S3FilterRule::builder()
                                .name(FilterRuleName::Suffix)
                                .value(".csv")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .build()
        .expect("queue config");

    let topic_config = S3TopicConfig::builder()
        .id("sns-filtered")
        .topic_arn(&topic_arn)
        .events(S3Event::S3ObjectRemoved)
        .filter(
            NotificationConfigurationFilter::builder()
                .key(
                    S3KeyFilter::builder()
                        .filter_rules(
                            S3FilterRule::builder()
                                .name(FilterRuleName::Prefix)
                                .value("")
                                .build(),
                        )
                        .filter_rules(
                            S3FilterRule::builder()
                                .name(FilterRuleName::Suffix)
                                .value(".txt")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .build()
        .expect("topic config");

    let put_result = s3
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            aws_sdk_s3::types::NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .topic_configurations(topic_config)
                .build(),
        )
        .send()
        .await;
    check("S3 PutBucketNotificationConfiguration with Filter", put_result.map(|_| ()).map_err(|e| e.to_string()));

    // GetBucketNotificationConfiguration — verify filter round-trip
    match s3
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
    {
        Ok(nc) => {
            let has_queue = nc.queue_configurations().iter().any(|q| {
                q.queue_arn() == queue_arn
            });
            let queue_filter_ok = nc.queue_configurations().iter().any(|q| {
                q.queue_arn() == queue_arn
                    && q.filter()
                        .and_then(|f| f.key())
                        .map(|k| k.filter_rules().len() == 2)
                        .unwrap_or(false)
            });
            let has_topic = nc.topic_configurations().iter().any(|t| {
                t.topic_arn() == topic_arn
            });
            let topic_filter_ok = nc.topic_configurations().iter().any(|t| {
                t.topic_arn() == topic_arn
                    && t.filter()
                        .and_then(|f| f.key())
                        .map(|k| k.filter_rules().len() == 2)
                        .unwrap_or(false)
            });
            check("S3 GetBucketNotificationConfiguration (queue)", if has_queue { ok() } else { Err("queue not found".into()) });
            check("S3 GetBucketNotificationConfiguration (queue filter round-trip)", if queue_filter_ok { ok() } else { Err("queue filter rules missing".into()) });
            check("S3 GetBucketNotificationConfiguration (topic)", if has_topic { ok() } else { Err("topic not found".into()) });
            check("S3 GetBucketNotificationConfiguration (topic filter round-trip)", if topic_filter_ok { ok() } else { Err("topic filter rules missing".into()) });
        }
        Err(e) => {
            check("S3 GetBucketNotificationConfiguration (queue)", err(&e));
            check("S3 GetBucketNotificationConfiguration (queue filter round-trip)", err(&e));
            check("S3 GetBucketNotificationConfiguration (topic)", err(&e));
            check("S3 GetBucketNotificationConfiguration (topic filter round-trip)", err(&e));
        }
    }

    // Cleanup
    let _ = s3.delete_bucket().bucket(bucket).send().await;
    let queue_url = format!("{}/{}/{}", endpoint, account_id, queue_name);
    let _ = sqs.delete_queue().queue_url(&queue_url).send().await;
    let _ = sns.delete_topic().topic_arn(&topic_arn).send().await;
}

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
        ("s3-cors", |ep| Box::pin(run_s3_cors(ep))),
        ("sts", |ep| Box::pin(run_sts(ep))),
        ("kms", |ep| Box::pin(run_kms(ep))),
        ("secretsmanager", |ep| Box::pin(run_secrets_manager(ep))),
        ("s3-notifications", |ep| Box::pin(run_s3_notifications(ep))),
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
