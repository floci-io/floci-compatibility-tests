# SDK Compatibility Tests - Task Runner Configuration
# Run `just` to see available commands

set export
set dotenv-load

# Environment defaults
FLOCI_ENDPOINT := env('FLOCI_ENDPOINT', 'http://localhost:4566')
AWS_ACCESS_KEY_ID := env('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY := env('AWS_SECRET_ACCESS_KEY', 'test')
AWS_DEFAULT_REGION := env('AWS_DEFAULT_REGION', 'us-east-1')

# Default recipe - list all available commands
default:
    @just --list

# Run all SDK tests sequentially (continues on failure)
test-all:
    #!/usr/bin/env bash
    set +e
    failed=0
    for suite in python typescript awscli java go rust; do
        echo "=== Running $suite tests ==="
        just test-$suite || failed=1
    done
    exit $failed

# Run Python SDK tests (TAP output)
[working-directory: 'sdk-test-python']
test-python:
    pytest tests/ --tap-stream

# Run TypeScript SDK tests (TAP output via vitest tap-flat reporter)
[working-directory: 'sdk-test-typescript']
test-typescript:
    npm test

# Run AWS CLI tests (TAP output via bats)
test-awscli:
    ./lib/bats-core/bin/bats --tap sdk-test-awscli/test/

# Run Java SDK tests (JUnit XML output via Maven Surefire)
[working-directory: 'sdk-test-java']
test-java:
    mvn test -q

# Run Go SDK tests (JUnit XML output via gotestsum)
[working-directory: 'sdk-test-go']
test-go:
    gotestsum --junitfile test-results.xml ./tests/...

# Run Rust SDK tests (JUnit XML output via cargo-nextest)
[working-directory: 'sdk-test-rust']
test-rust:
    cargo nextest run --profile ci

# Install all test dependencies
setup: setup-python setup-typescript setup-awscli setup-java setup-go setup-rust

# Install Python test dependencies
[working-directory: 'sdk-test-python']
setup-python:
    pip install -r requirements.txt

# Install TypeScript test dependencies
[working-directory: 'sdk-test-typescript']
setup-typescript:
    npm install

# Install Java test dependencies
[working-directory: 'sdk-test-java']
setup-java:
    mvn dependency:resolve -q

# Install Go test dependencies
[working-directory: 'sdk-test-go']
setup-go:
    go mod download
    go install gotest.tools/gotestsum@latest

# Install Rust test dependencies
[working-directory: 'sdk-test-rust']
setup-rust:
    cargo fetch
    cargo install cargo-nextest --locked

# Install AWS CLI test dependencies (uses shared bats from repo root)
setup-awscli: setup-bats
    @echo "AWS CLI test dependencies ready (using shared bats)"

# Install bats-core and helpers for compat tests
setup-bats:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p lib
    if [ ! -d "lib/bats-core" ]; then
        echo "Cloning bats-core..."
        git clone --depth 1 https://github.com/bats-core/bats-core.git lib/bats-core
    fi
    if [ ! -d "lib/bats-support" ]; then
        echo "Cloning bats-support..."
        git clone --depth 1 https://github.com/bats-core/bats-support.git lib/bats-support
    fi
    if [ ! -d "lib/bats-assert" ]; then
        echo "Cloning bats-assert..."
        git clone --depth 1 https://github.com/bats-core/bats-assert.git lib/bats-assert
    fi
    echo "Bats dependencies installed!"

# Run CDK compatibility tests (TAP output via bats)
test-cdk:
    ./lib/bats-core/bin/bats --tap compat-cdk/test/

# Run Terraform compatibility tests (TAP output via bats)
test-terraform:
    ./lib/bats-core/bin/bats --tap compat-terraform/test/

# Run OpenTofu compatibility tests (TAP output via bats)
test-opentofu:
    ./lib/bats-core/bin/bats --tap compat-opentofu/test/

# Run all IaC compatibility tests (continues on failure)
test-compat:
    #!/usr/bin/env bash
    set +e
    failed=0
    for suite in cdk terraform opentofu; do
        echo "=== Running $suite tests ==="
        just test-$suite || failed=1
    done
    exit $failed

# Remove build artifacts and dependencies
clean:
    rm -rf sdk-test-python/__pycache__ sdk-test-python/.pytest_cache sdk-test-python/test-results
    rm -rf sdk-test-typescript/node_modules sdk-test-typescript/dist sdk-test-typescript/test-results
    rm -rf sdk-test-java/target
    rm -rf sdk-test-go/test-results.xml
    rm -rf sdk-test-rust/target
    rm -rf lib/bats-core lib/bats-support lib/bats-assert
