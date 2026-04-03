package com.floci.test.tests;

import java.nio.charset.StandardCharsets;

/** Shared Lambda deployment-package helpers. */
public class LambdaUtils {

    /**
     * ZIP containing a Node.js handler that greets by name and echoes the event.
     */
    public static byte[] handlerZip() {
        String code = """
                exports.handler = async (event) => {
                    const name = (event && event.name) ? event.name : 'World';
                    console.log('[handler] invoked with event:', JSON.stringify(event));
                    console.log('[handler] resolved name:', name);
                    const response = {
                        statusCode: 200,
                        body: JSON.stringify({ message: `Hello, ${name}!`, input: event })
                    };
                    console.log('[handler] returning response:', JSON.stringify(response));
                    return response;
                };
                """;
        try {
            var baos = new java.io.ByteArrayOutputStream();
            try (var zos = new java.util.zip.ZipOutputStream(baos)) {
                zos.putNextEntry(new java.util.zip.ZipEntry("index.js"));
                zos.write(code.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build handler ZIP", e);
        }
    }

    /**
     * ZIP containing a Ruby handler that greets by name.
     */
    public static byte[] rubyZip() {
        String code = """
                def lambda_handler(event:, context:)
                  name = event['name'] || 'World'
                  { statusCode: 200, body: "Hello, #{name}!" }
                end
                """;
        try {
            var baos = new java.io.ByteArrayOutputStream();
            try (var zos = new java.util.zip.ZipOutputStream(baos)) {
                zos.putNextEntry(new java.util.zip.ZipEntry("lambda_function.rb"));
                zos.write(code.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Ruby ZIP", e);
        }
    }

    /**
     * ZIP containing a {@code bootstrap} shell script that implements the Lambda Runtime API.
     * Used to test {@code provided.al2023} / {@code provided.al2} custom runtimes.
     * The script calls {@code /invocation/next}, then posts {@code "hello from provided runtime"}
     * as the response.
     */
    public static byte[] providedRuntimeZip() {
        String bootstrap = """
                #!/bin/sh
                ENDPOINT="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
                while true; do
                  HEADERS=$(mktemp)
                  curl -sS -D "$HEADERS" -o /tmp/event.json "${ENDPOINT}/invocation/next"
                  REQUEST_ID=$(grep -i 'lambda-runtime-aws-request-id' "$HEADERS" | tr -d '\\r' | awk '{print $2}')
                  curl -sS -X POST "${ENDPOINT}/invocation/${REQUEST_ID}/response" \\
                    -H 'Content-Type: application/json' \\
                    -d '"hello from provided runtime"'
                  rm -f "$HEADERS"
                done
                """;
        try {
            var baos = new java.io.ByteArrayOutputStream();
            try (var zos = new java.util.zip.ZipOutputStream(baos)) {
                zos.putNextEntry(new java.util.zip.ZipEntry("bootstrap"));
                zos.write(bootstrap.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build provided-runtime bootstrap ZIP", e);
        }
    }

    /**
     * Minimal valid ZIP containing a stub {@code index.js} — accepted by the emulator
     * without needing a real runtime.
     */
    public static byte[] minimalZip() {
        String code = """
                exports.handler = async (event) => {
                    console.log('[esm-handler] invoked with event:', JSON.stringify(event));
                    return { statusCode: 200, body: 'ok' };
                };
                """;
        try {
            var baos = new java.io.ByteArrayOutputStream();
            try (var zos = new java.util.zip.ZipOutputStream(baos)) {
                zos.putNextEntry(new java.util.zip.ZipEntry("index.js"));
                zos.write(code.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build minimal ZIP", e);
        }
    }
}
