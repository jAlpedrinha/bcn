#!/bin/bash
#
# Local Development Setup for Iceberg Snapshots
#
# This script sets up your local environment to run tests without Docker containers
#

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "Iceberg Snapshots - Local Development Setup"
echo "========================================================================"

# Step 1: Check Java
echo -e "\n${BLUE}Step 1: Checking Java installation...${NC}"
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    echo -e "${GREEN}✓ Java found: $JAVA_VERSION${NC}"

    # Check if it's Java 11 or higher
    JAVA_MAJOR=$(echo "$JAVA_VERSION" | cut -d'.' -f1)
    if [ "$JAVA_MAJOR" -lt 11 ]; then
        echo -e "${RED}✗ Java 11 or higher is required${NC}"
        echo -e "${YELLOW}Install with: brew install openjdk@11${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Java not found${NC}"
    echo -e "${YELLOW}Install with: brew install openjdk@11${NC}"
    exit 1
fi

# Step 2: Check Python and uv
echo -e "\n${BLUE}Step 2: Checking Python and uv...${NC}"
if command -v uv &> /dev/null; then
    echo -e "${GREEN}✓ uv found${NC}"
else
    echo -e "${YELLOW}! uv not found, installing...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Step 3: Install Python dependencies
echo -e "\n${BLUE}Step 3: Installing Python dependencies...${NC}"
uv sync
echo -e "${GREEN}✓ Python dependencies installed${NC}"

# Step 4: Download Iceberg JARs
echo -e "\n${BLUE}Step 4: Setting up Iceberg JARs...${NC}"
JARS_DIR="$HOME/.bcn/jars"
mkdir -p "$JARS_DIR"

ICEBERG_VERSION="1.4.3"
SPARK_VERSION="3.5"
SCALA_VERSION="2.12"

ICEBERG_JAR="iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
JAR_PATH="$JARS_DIR/$ICEBERG_JAR"

if [ ! -f "$JAR_PATH" ]; then
    echo -e "${YELLOW}Downloading Iceberg runtime JAR...${NC}"
    MAVEN_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/${ICEBERG_JAR}"

    curl -L "$MAVEN_URL" -o "$JAR_PATH" --progress-bar
    echo -e "${GREEN}✓ Downloaded Iceberg JAR${NC}"
else
    echo -e "${GREEN}✓ Iceberg JAR already exists${NC}"
fi

# Step 5: Create environment setup file
echo -e "\n${BLUE}Step 5: Creating environment configuration...${NC}"

# Detect Docker socket location (for macOS Docker Desktop compatibility)
DOCKER_SOCK_PATH="/var/run/docker.sock"
if [ -S "$HOME/.docker/run/docker.sock" ]; then
    DOCKER_SOCK_PATH="$HOME/.docker/run/docker.sock"
fi

cat > .env.local <<EOF
# Iceberg Snapshots - Local Development Environment
# Source this file before running tests: source .env.local

# Docker Configuration (macOS Docker Desktop)
export DOCKER_HOST="unix://${DOCKER_SOCK_PATH}"

# S3/MinIO Configuration
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY="admin"
export S3_SECRET_KEY="password"
export S3_REGION="us-east-1"

# Hive Metastore Configuration
export HIVE_METASTORE_URI="thrift://localhost:9083"

# Backup Configuration
export BACKUP_BUCKET="iceberg"
export WAREHOUSE_BUCKET="warehouse"

# Spark/Iceberg Configuration
export SPARK_HOME="\$(python -c 'import pyspark; import os; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_SUBMIT_ARGS="--jars $JAR_PATH --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions pyspark-shell"

# Python path
export PYTHONPATH="\${PYTHONPATH}:${PROJECT_ROOT}/src"

echo "✓ Iceberg Snapshots local environment loaded"
EOF

echo -e "${GREEN}✓ Environment file created: .env.local${NC}"

# Step 6: Check infrastructure
echo -e "\n${BLUE}Step 6: Checking Docker infrastructure...${NC}"
if docker ps | grep -q "hive-metastore"; then
    echo -e "${GREEN}✓ Infrastructure is running${NC}"
else
    echo -e "${YELLOW}! Infrastructure not running${NC}"
    echo -e "${YELLOW}Starting infrastructure...${NC}"
    docker-compose up -d

    echo -e "${YELLOW}Waiting for services to be healthy (30s)...${NC}"
    sleep 30

    if docker ps | grep -q "hive-metastore"; then
        echo -e "${GREEN}✓ Infrastructure started${NC}"
    else
        echo -e "${RED}✗ Failed to start infrastructure${NC}"
        exit 1
    fi
fi

# Step 7: Summary
echo -e "\n========================================================================"
echo -e "${GREEN}✓✓✓ Local Development Setup Complete! ✓✓✓${NC}"
echo -e "========================================================================"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo ""
echo -e "1. Load the environment:"
echo -e "   ${YELLOW}source .env.local${NC}"
echo ""
echo -e "2. Run tests locally:"
echo -e "   ${YELLOW}uv run pytest tests/ -v${NC}"
echo ""
echo -e "3. Run specific test:"
echo -e "   ${YELLOW}uv run pytest tests/test_backup_restore.py::TestBackupRestore::test_complete_backup_restore_workflow -v${NC}"
echo ""
echo -e "4. Run backup/restore scripts:"
echo -e "   ${YELLOW}uv run python src/backup.py --database default --table my_table --backup-name backup1${NC}"
echo -e "   ${YELLOW}uv run python src/restore.py --backup-name backup1 --target-database default --target-table my_table_2 --target-location s3://warehouse/default/my_table_2${NC}"
echo ""
echo -e "${BLUE}Useful commands:${NC}"
echo -e "  - Check infrastructure: ${YELLOW}docker ps${NC}"
echo -e "  - View MinIO console: ${YELLOW}http://localhost:9001${NC} (admin/password)"
echo -e "  - Stop infrastructure: ${YELLOW}docker-compose down${NC}"
echo ""
echo "========================================================================"
