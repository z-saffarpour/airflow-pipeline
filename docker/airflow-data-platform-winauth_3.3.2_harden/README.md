# Airflow SQL Server + Kafka Pipeline with Windows Authentication

A complete Docker Compose setup for Apache Airflow with SQL Server, Kafka, and Kerberos (Windows Authentication) integration.

## 📋 Overview

This project provides a production-ready Airflow deployment with:

- ✓ **Apache Airflow 3.0.6** with Celery distributed execution
- ✓ **FastAPI REST API** for programmatic access
- ✓ **PostgreSQL** for metadata database
- ✓ **Redis** for task queue
- ✓ **Kerberos Authentication** for Windows/Active Directory integration
- ✓ **SQL Server Support** via ODBC driver
- ✓ **Kafka Integration** for data streaming
- ✓ **Security Hardening** with environment-based secrets
- ✓ **Automated Backups** for PostgreSQL
- ✓ **Health Checks** for all services

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose (latest)
- Python 3.8+ (for key generation)
- OpenSSL (for password generation)
- 4GB+ RAM available
- 10GB+ free disk space

### 1. Initial Setup

#### Option A: Automated Setup (Recommended)

**Linux/macOS:**
```bash
bash setup-security.sh
```

**Windows:**
```bash
setup-security.bat
```

#### Option B: Manual Setup

```bash
# Copy environment template
cp .env.example .env

# Generate secure keys
python -c "from cryptography.fernet import Fernet; print('AIRFLOW__CORE__FERNET_KEY=' + Fernet.generate_key().decode())"

# Edit .env with strong passwords
nano .env  # or use your editor

# Secure the file
chmod 600 .env
```

### 2. Configure Your Environment

Edit `.env` file and update:

```env
# Database
POSTGRES_PASSWORD=YourStrongPassword123!
POSTGRES_USER=airflow

# Redis
REDIS_PASSWORD=YourStrongRedisPassword123!

# SQL Server (optional, if using external MSSQL)
MSSQL_HOST=your-sqlserver.okco.ir
MSSQL_USERNAME=domain\\username
MSSQL_PASSWORD=your_password

# Kerberos
# Already configured for OKCO.IR domain
```

### 3. Verify Kerberos Configuration

Edit `config/krb5.conf` if needed:

```ini
[libdefaults]
    default_realm = OKCO.IR
    dns_lookup_realm = false
    dns_lookup_kdc = true

[realms]
    OKCO.IR = {
        kdc = okdc10003.okco.ir
        admin_server = okdc10003.okco.ir
        default_domain = okco.ir
    }
```

### 4. Start Services

**Development:**
```bash
docker-compose -f docker-compose.winauth.yml up -d
```

**Production (with security hardening):**
```bash
docker-compose -f docker-compose.winauth.yml -f docker-compose.prod.yml up -d
```

### 5. Monitor Startup

```bash
# Check service status
docker-compose -f docker-compose.winauth.yml ps

# View logs
docker-compose -f docker-compose.winauth.yml logs -f airflow-apiserver

# Wait for services to be healthy (2-3 minutes)
```

### 6. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:9280 | airflow/airflow* |
| API Server | http://localhost:9280/api | JWT Token |
| Flower (Celery) | http://localhost:9555 | None (development) |
| PostgreSQL | localhost:9432 | See .env |

*Default credentials are generated during first run. Update them in Airflow UI.

## 📁 Project Structure

```
.
├── docker-compose.winauth.yml         # Main Docker Compose file
├── docker-compose.prod.yml            # Production security overrides
├── .env.example                        # Environment template
├── setup-security.sh                   # Linux/macOS setup script
├── setup-security.bat                  # Windows setup script
├── airflow_api_examples.sh             # Bash/cURL API examples
├── SECURITY_GUIDE.md                   # Security documentation
├── DEPLOYMENT_CHECKLIST.md             # Pre/post deployment checklist
├── README.md                           # This file
└── config/
    ├── airflow.cfg                    # Airflow configuration
    └── krb5.conf                      # Kerberos configuration
```

## 🔒 Security Features

### Implemented

- ✓ Secrets management via `.env` file
- ✓ Fernet key encryption for sensitive data
- ✓ JWT authentication for API
- ✓ RBAC (Role-Based Access Control) enabled
- ✓ Redis password authentication
- ✓ Secure HTTP cookies
- ✓ Network isolation via Docker bridge network
- ✓ File permission restrictions
- ✓ Kerberos/Windows AD integration

### Best Practices

See [API_GUIDE.md](API_GUIDE.md) for:
- Complete REST API documentation
- cURL and Python examples
- Postman collection for testing
- Common integration patterns

## 🔧 Configuration

### Environment Variables

All configuration is managed via `.env` file. Key variables:

```env
# Core
# Set AIRFLOW_UID to your host user's UID before starting Docker Compose.
# Example (Linux): export AIRFLOW_UID=$(id -u)
AIRFLOW_UID=<your_host_uid>
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# Secrets (generate with setup script)
AIRFLOW__CORE__FERNET_KEY=<generated>
AIRFLOW__API_AUTH__JWT_SECRET=<generated>
AIRFLOW__WEBSERVER__SECRET_KEY=<generated>

# Database
POSTGRES_PASSWORD=<strong_password>
POSTGRES_USER=airflow
REDIS_PASSWORD=<strong_password>

# Timezone
TZ=Asia/Tehran
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Tehran

# Kerberos
KRB5_CONFIG=/etc/krb5.conf
```

See `.env.example` for all available options.

### Airflow Configuration

Edit `config/airflow.cfg` for detailed Airflow settings:

```ini
[core]
executor = LocalExecutor
auth_manager = airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
load_examples = False
dags_folder = /opt/airflow/dags

[security]
rbac = True

[webserver]
expose_config = False
secure_cookies = True
```

## 📡 REST API Configuration

Airflow 3.0.6 includes a powerful **FastAPI-based REST API** for programmatic access.

### API Documentation

- **Swagger UI**: http://localhost:9280/api/v1/ui
- **ReDoc**: http://localhost:9280/api/v1/redoc

### Get API Token

```bash
curl -X POST "http://localhost:9280/api/v1/security/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}'
```

### Common API Endpoints

**List DAGs:**
```bash
curl -H "Authorization: Bearer ${TOKEN}" \
  "http://localhost:9280/api/v1/dags"
```

**Trigger DAG Run:**
```bash
curl -X POST "http://localhost:9280/api/v1/dags/{dag_id}/dagRuns" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"key": "value"}}'
```

**Get DAG Run Status:**
```bash
curl -H "Authorization: Bearer ${TOKEN}" \
  "http://localhost:9280/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
```

### Integration Examples

See [API_GUIDE.md](API_GUIDE.md) for:
- Complete REST API documentation
- Python client examples
- JavaScript/Node.js examples
- Postman collection
- Common integration patterns

## 📊 Monitoring & Logs

### View Logs

```bash
# All services
docker-compose -f docker-compose.winauth.yml logs

# Specific service
docker-compose -f docker-compose.winauth.yml logs airflow-scheduler

# Follow logs
docker-compose -f docker-compose.winauth.yml logs -f airflow-apiserver
```

### Health Checks

```bash
# Check service health
docker-compose -f docker-compose.winauth.yml ps

# Manual health check
curl http://localhost:9280/health
```

### Access Logs Directory

```bash
# From host
ls -la logs/

# From container
docker-compose -f docker-compose.winauth.yml exec airflow-scheduler ls -la /opt/airflow/logs/
```

## 🗄️ Database Management

### PostgreSQL Access

```bash
# Connect to PostgreSQL
docker-compose -f docker-compose.winauth.yml exec postgres \
  psql -U airflow -d airflow

# Run SQL commands
docker-compose -f docker-compose.winauth.yml exec postgres \
  psql -U airflow -d airflow -c "SELECT * FROM dag;"
```

### Database Backups

Backups are automatically created daily at 2 AM:

```bash
# View backups
ls -la postgres-backups/

# Restore from backup
docker-compose -f docker-compose.winauth.yml exec postgres \
  pg_restore -U airflow -d airflow /backups/backup_file.sql.gz
```

## 🐛 Troubleshooting

### Services Won't Start

```bash
# Check resource availability
free -h                          # Linux
docker stats                    # All platforms

# Check logs for errors
docker-compose -f docker-compose.winauth.yml logs

# Increase Docker resources (Windows/macOS)
# Docker Desktop → Settings → Resources
```

### Redis Connection Error

```bash
# Test Redis connection
docker-compose -f docker-compose.winauth.yml exec redis \
  redis-cli -a ${REDIS_PASSWORD} ping

# Check Redis password in .env
grep REDIS_PASSWORD .env
```

### PostgreSQL Connection Error

```bash
# Test PostgreSQL connection
docker-compose -f docker-compose.winauth.yml exec postgres \
  psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "SELECT version();"

# Check connection string
docker-compose -f docker-compose.winauth.yml exec airflow-scheduler \
  echo $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
```

### Kerberos Issues

```bash
# Check Kerberos configuration
docker-compose -f docker-compose.winauth.yml exec airflow-scheduler \
  cat /etc/krb5.conf

# Test Kerberos connectivity
docker-compose -f docker-compose.winauth.yml exec airflow-scheduler \
  kinit -V

# Check Kerberos logs
docker-compose -f docker-compose.winauth.yml exec airflow-scheduler \
  cat /var/log/krb5kdc.log
```

### UI Not Accessible

```bash
# Check if port is in use
netstat -an | grep 9280      # Linux/macOS
netstat -ano | findstr :9280  # Windows

# Use different port in docker-compose
# Change: ports: - "9280:8080" to - "8080:8080"
```

## 🚀 Scaling & Performance

### Scale Workers

```bash
# Scale to 6 workers
docker-compose -f docker-compose.winauth.yml up -d --scale airflow-worker=6

# Check running workers
docker-compose -f docker-compose.winauth.yml ps | grep airflow-worker
```

### Resource Limits

Edit `docker-compose.prod.yml` to set limits:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 2G
    reservations:
      cpus: '1'
      memory: 1G
```

### Database Optimization

PostgreSQL is pre-configured with:
- 400 connections
- 2GB shared buffers
- 6GB effective cache size

Adjust in `docker-compose.winauth.yml` postgres command if needed.

## 🛑 Stopping & Cleanup

### Stop Services

```bash
# Stop all services (data persists)
docker-compose -f docker-compose.winauth.yml stop

# Stop and remove containers
docker-compose -f docker-compose.winauth.yml down

# Remove volumes (WARNING: data loss)
docker-compose -f docker-compose.winauth.yml down -v
```

### Clean Up

```bash
# Remove unused Docker resources
docker system prune

# Remove unused volumes
docker volume prune

# Remove all Airflow data
rm -rf logs postgres-data postgres-backups
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow REST API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [API_GUIDE.md](API_GUIDE.md) - Complete API guide with examples
- [SECURITY_GUIDE.md](SECURITY_GUIDE.md) - Security configuration guide
- [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) - Pre/post deployment checklist
- [Airflow Security](https://airflow.apache.org/docs/apache-airflow/stable/security-and-api/security/)
- [Kerberos Authentication](https://airflow.apache.org/docs/apache-airflow/stable/security-and-api/kerberos/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
- [PostgreSQL Docker Image](https://hub.docker.com/_/postgres)
- [Redis Docker Image](https://hub.docker.com/_/redis)

## 📝 License

This project is provided as-is for use within OKCO organization.

## 🤝 Support

For issues or questions:
1. Check [SECURITY_GUIDE.md](SECURITY_GUIDE.md)
2. Review logs: `docker-compose logs [service-name]`
3. Consult Airflow documentation
4. Contact your DevOps team

---

**Last Updated:** 2026-05-24  
**Version:** 1.0.0  
**Status:** Production Ready
