#!/bin/bash
# Integration Test Runner for dbkrab
# Usage: ./run-integration-tests.sh [options]
# Options:
#   --clean    Clean up containers after tests
#   --no-build Skip building the test image
#   --help     Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

CLEAN=false
NO_BUILD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        --no-build)
            NO_BUILD=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --clean    Clean up containers after tests"
            echo "  --no-build Skip building the test image"
            echo "  --help     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== dbkrab Integration Tests ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# Build test image if not skipped
if [ "$NO_BUILD" = false ]; then
    echo "Building test image..."
    cd "$PROJECT_ROOT"
    docker build -f tests/integration/Dockerfile.test -t dbkrab-test-runner .
    echo ""
fi

# Start MSSQL and run tests
echo "Starting test environment..."
cd "$SCRIPT_DIR"

# Check if docker-compose is available
if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
else
    echo "Error: docker-compose not found"
    exit 1
fi

# Start services and run tests
$COMPOSE_CMD up --build --abort-on-container-exit --exit-code-from dbkrab-test
TEST_EXIT_CODE=$?

# Cleanup if requested
if [ "$CLEAN" = true ]; then
    echo ""
    echo "Cleaning up containers..."
    $COMPOSE_CMD down -v
fi

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "=== Integration Tests PASSED ==="
else
    echo "=== Integration Tests FAILED (exit code: $TEST_EXIT_CODE) ==="
fi

exit $TEST_EXIT_CODE
