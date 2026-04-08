#!/usr/bin/env bash
# Common setup for CDK bats tests

# Get repository root (3 levels up from test_helper)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Load bats helpers
load "${REPO_ROOT}/lib/bats-support/load"
load "${REPO_ROOT}/lib/bats-assert/load"

# Load shared compat helpers
source "${REPO_ROOT}/lib/compat-common.bash"

# CDK-specific environment
export LOCALSTACK_HOSTNAME="${LOCALSTACK_HOSTNAME:-localhost}"
export EDGE_PORT="${EDGE_PORT:-4566}"

# Override endpoint for Docker networking if needed
if [ "$LOCALSTACK_HOSTNAME" = "floci" ]; then
    export FLOCI_ENDPOINT="http://floci:4566"
    export AWS_ENDPOINT_URL="http://floci:4566"
fi
