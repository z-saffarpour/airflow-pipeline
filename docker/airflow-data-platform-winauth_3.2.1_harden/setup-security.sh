#!/bin/bash
# =============================================================================
# Airflow Security Setup Script
# =============================================================================
# This script helps with initial security configuration
# Usage: bash setup-security.sh
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==============================================================================${NC}"
echo -e "${BLUE}Airflow Security Setup${NC}"
echo -e "${BLUE}==============================================================================${NC}\n"

# Check if .env exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}[1/5] Creating .env file from template...${NC}"
    if [ -f .env.example ]; then
        cp .env.example .env
        echo -e "${GREEN}✓ .env created${NC}"
    else
        echo -e "${RED}✗ .env.example not found!${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}[1/5] .env file already exists (skipping)${NC}"
fi

# Function to generate a strong password
generate_password() {
    openssl rand -base64 32 | tr -d '\n'
}

# Function to generate Fernet key
generate_fernet_key() {
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "ERROR"
}

# Function to update .env value
update_env_value() {
    local key=$1
    local value=$2
    local file=.env
    
    # Check OS and use appropriate sed syntax
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|^${key}=.*|${key}=${value}|g" "$file"
    else
        sed -i "s|^${key}=.*|${key}=${value}|g" "$file"
    fi
}

# Check Python availability
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 is required for key generation${NC}"
    echo -e "${YELLOW}Please install Python 3 or generate keys manually using:${NC}"
    echo "  python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    exit 1
fi

# Generate security keys
echo -e "\n${YELLOW}[2/5] Generating security keys...${NC}"

echo -e "${BLUE}Generating FERNET_KEY...${NC}"
FERNET_KEY=$(generate_fernet_key)
if [ "$FERNET_KEY" != "ERROR" ]; then
    update_env_value "AIRFLOW__CORE__FERNET_KEY" "$FERNET_KEY"
    echo -e "${GREEN}✓ FERNET_KEY generated${NC}"
else
    echo -e "${RED}✗ Failed to generate FERNET_KEY${NC}"
    exit 1
fi

echo -e "${BLUE}Generating JWT_SECRET...${NC}"
JWT_SECRET=$(python3 -c "import os; print(os.urandom(32).hex())")
update_env_value "AIRFLOW__API_AUTH__JWT_SECRET" "$JWT_SECRET"
echo -e "${GREEN}✓ JWT_SECRET generated${NC}"

echo -e "${BLUE}Generating WEBSERVER_SECRET_KEY...${NC}"
WEBSERVER_SECRET=$(python3 -c "import os; print(os.urandom(32).hex())")
update_env_value "AIRFLOW__WEBSERVER__SECRET_KEY" "$WEBSERVER_SECRET"
echo -e "${GREEN}✓ WEBSERVER_SECRET_KEY generated${NC}"

# Generate database passwords
echo -e "\n${YELLOW}[3/5] Generating database passwords...${NC}"

echo -e "${BLUE}Generating POSTGRES_PASSWORD...${NC}"
POSTGRES_PASS=$(generate_password)
update_env_value "POSTGRES_PASSWORD" "$POSTGRES_PASS"
echo -e "${GREEN}✓ POSTGRES_PASSWORD generated${NC}"

echo -e "${BLUE}Generating REDIS_PASSWORD...${NC}"
REDIS_PASS=$(generate_password)
update_env_value "REDIS_PASSWORD" "$REDIS_PASS"
echo -e "${GREEN}✓ REDIS_PASSWORD generated${NC}"

# Set file permissions
echo -e "\n${YELLOW}[4/5] Setting file permissions...${NC}"

echo -e "${BLUE}Securing .env file (chmod 600)...${NC}"
chmod 600 .env
echo -e "${GREEN}✓ .env permissions set (readable by owner only)${NC}"

echo -e "${BLUE}Securing config directory...${NC}"
if [ -d config ]; then
    chmod 700 config
    chmod 600 config/* 2>/dev/null || true
    echo -e "${GREEN}✓ config/ permissions set${NC}"
else
    echo -e "${YELLOW}⚠ config/ directory not found${NC}"
fi

# Verify .gitignore
echo -e "\n${YELLOW}[5/5] Verifying .gitignore...${NC}"

if grep -q "^\.env$" .gitignore 2>/dev/null; then
    echo -e "${GREEN}✓ .env is in .gitignore${NC}"
else
    echo -e "${YELLOW}⚠ Adding .env to .gitignore${NC}"
    echo ".env" >> .gitignore 2>/dev/null || true
fi

# Summary
echo -e "\n${BLUE}==============================================================================${NC}"
echo -e "${GREEN}✓ Security setup completed!${NC}"
echo -e "${BLUE}==============================================================================${NC}\n"

echo -e "${YELLOW}IMPORTANT NEXT STEPS:${NC}"
echo "1. Review and update .env file with your specific configuration:"
echo "   - MSSQL_HOST: Your SQL Server hostname"
echo "   - MSSQL_USERNAME: SQL Server username"
echo "   - POSTGRES_USER: Database user (if changing from 'airflow')"
echo "   - Other application-specific settings"
echo ""
echo "2. Verify Kerberos configuration:"
echo "   - Review config/krb5.conf"
echo "   - Ensure KDC is accessible: okdc10003.okco.ir"
echo ""
echo "3. Start the services:"
echo "   docker-compose -f docker-compose.winauth.yml up -d"
echo ""
echo "4. Access Airflow UI:"
echo "   http://localhost:9280"
echo ""
echo "5. Default credentials (set during first run):"
echo "   Username: airflow"
echo "   Password: Check _AIRFLOW_WWW_USER_PASSWORD in .env"
echo ""
echo -e "${YELLOW}For detailed security information, see SECURITY_GUIDE.md${NC}\n"

# Check for required environment variables
echo -e "${YELLOW}Checking critical environment variables...${NC}"
CRITICAL_MISSING=0

check_env_var() {
    local var=$1
    local current_value=$(grep "^${var}=" .env | cut -d= -f2- | tr -d "'\"")
    
    if [[ "$current_value" =~ "ChangeMe" ]]; then
        echo -e "${RED}✗ ${var} still has default value${NC}"
        CRITICAL_MISSING=1
    elif [ -z "$current_value" ]; then
        echo -e "${RED}✗ ${var} is not set${NC}"
        CRITICAL_MISSING=1
    else
        echo -e "${GREEN}✓ ${var} is set${NC}"
    fi
}

check_env_var "AIRFLOW__CORE__FERNET_KEY"
check_env_var "AIRFLOW__API_AUTH__JWT_SECRET"
check_env_var "POSTGRES_PASSWORD"
check_env_var "REDIS_PASSWORD"

if [ $CRITICAL_MISSING -eq 1 ]; then
    echo -e "\n${RED}⚠ Some critical variables are missing or have default values!${NC}"
    echo -e "${YELLOW}Please update .env manually and re-run this script if needed.${NC}"
fi

echo -e "\n${GREEN}Setup script completed successfully!${NC}\n"
