# Security & Deployment Checklist

## Pre-Deployment Checklist

### 🔒 Security Configuration

- [ ] Run security setup script
  - [ ] Linux/macOS: `bash setup-security.sh`
  - [ ] Windows: `setup-security.bat`
- [ ] Verify `.env` file created and secured (chmod 600)
- [ ] Verify all secrets generated:
  - [ ] `AIRFLOW__CORE__FERNET_KEY` is set (not "ChangeMe_*")
  - [ ] `AIRFLOW__API_AUTH__JWT_SECRET` is set
  - [ ] `AIRFLOW__WEBSERVER__SECRET_KEY` is set
  - [ ] `POSTGRES_PASSWORD` is strong (16+ chars)
  - [ ] `REDIS_PASSWORD` is strong (16+ chars)
- [ ] `.env` is in `.gitignore`
- [ ] No secrets committed to Git
- [ ] `.env` permissions are restrictive (owner read-only)

### 🗝️ Kerberos/Active Directory

- [ ] Verify Kerberos realm is correct: `OKCO.IR`
- [ ] Verify KDC is accessible: `okdc10003.okco.ir`
- [ ] Test DNS resolution:
  ```bash
  nslookup okdc10003.okco.ir
  ```
- [ ] Verify `config/krb5.conf` is correctly configured
- [ ] Test Kerberos connectivity from container:
  ```bash
  docker-compose exec airflow-scheduler kinit -V username@OKCO.IR
  ```

### 🗄️ SQL Server

- [ ] SQL Server hostname configured in `.env`
- [ ] MSSQL_USERNAME and MSSQL_PASSWORD set (if external)
- [ ] Windows Authentication driver installed (ODBC Driver 17)
- [ ] SQL Server is accessible from Docker network
- [ ] Firewall rules allow connection to port 1433

### 🐘 PostgreSQL

- [ ] Database user changed from default (if production)
- [ ] Strong password set in `POSTGRES_PASSWORD`
- [ ] Database backup location is writable: `postgres-backups/`
- [ ] Backup retention policy configured (default: 7 days)
- [ ] Backup schedule verified (default: 2 AM daily)

### 🔴 Redis

- [ ] Redis password set in `REDIS_PASSWORD`
- [ ] Password is strong (24+ bytes base64)
- [ ] AOF persistence enabled
- [ ] Memory limit configured (1GB)
- [ ] LRU eviction policy configured

### 🌐 Network & Firewall

- [ ] Docker network created: `airflow_net`
- [ ] Subnet configured: `172.20.0.0/16`
- [ ] Firewall rules configured:
  - [ ] Port 9280 (Airflow UI): Open to required hosts
  - [ ] Port 9432 (PostgreSQL): Closed from external
  - [ ] Port 9555 (Flower): Closed from external
  - [ ] Port 6379 (Redis): Closed from external
- [ ] No services exposed to public internet (unless intended)

### 📝 Configuration Files

- [ ] `config/airflow.cfg` reviewed
  - [ ] `executor = CeleryExecutor`
  - [ ] `auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`
  - [ ] `rbac = true`
  - [ ] `expose_config = false`
  - [ ] `secure_cookies = true`
  - [ ] Timezone set to `Asia/Tehran`

### 💾 Resource Allocation

- [ ] 4GB+ RAM available for Docker
- [ ] 10GB+ free disk space
- [ ] CPU cores: 2+ recommended
- [ ] Storage for logs: 50GB+ recommended (adjust retention in airflow.cfg)

### 📦 Docker Images

- [ ] All images pulled/built successfully
  - [ ] Airflow image: `airflow-sqlserver-kafka-winauth-timezone:3.0.6`
  - [ ] PostgreSQL: `postgres:15-alpine`
  - [ ] Redis: `redis:alpine`
- [ ] No image pull errors
- [ ] Disk space sufficient for images

---

## Deployment Checklist

### 🚀 Services Startup

- [ ] Start services: `docker-compose -f docker-compose.winauth.yml up -d`
- [ ] Wait 2-3 minutes for services to initialize
- [ ] Check all services are running: `docker-compose ps`
  - [ ] postgres: healthy
  - [ ] redis: healthy
  - [ ] airflow-apiserver: healthy
  - [ ] airflow-scheduler: up
  - [ ] airflow-dag-processor: up
  - [ ] airflow-worker: up (multiple instances)
  - [ ] airflow-triggerer: up
  - [ ] postgres-backup: up

### ✅ Service Health

- [ ] Test Airflow API:
  ```bash
  curl http://localhost:9280/api/v2/version
  ```
- [ ] Test PostgreSQL:
  ```bash
  docker-compose exec postgres psql -U airflow -c "SELECT 1;"
  ```
- [ ] Test Redis:
  ```bash
  docker-compose exec redis redis-cli -a ${REDIS_PASSWORD} ping
  ```
- [ ] Check logs for errors:
  ```bash
  docker-compose logs | grep -i error
  ```

### 🔐 Security Verification

- [ ] Verify secrets are not in logs:
  ```bash
  docker-compose logs | grep -i "fernet\|jwt\|secret"
  ```
- [ ] Verify .env is not in container:
  ```bash
  docker-compose exec airflow-scheduler ls -la / | grep env
  ```
- [ ] Verify file permissions:
  ```bash
  ls -la .env config/
  ```

### 🎯 UI & Access

- [ ] Access Airflow UI: http://localhost:9280
- [ ] Login with default credentials (airflow/airflow)
- [ ] Verify DAGs folder is accessible
- [ ] Check no default example DAGs are loaded
- [ ] Verify timezone is Asia/Tehran

### 📊 Monitoring Setup

- [ ] Set up log aggregation (if using ELK, Splunk, etc.)
- [ ] Set up alerting for:
  - [ ] Service failures
  - [ ] DAG failures
  - [ ] Resource exhaustion
  - [ ] Database connection errors
- [ ] Set up metrics collection (Prometheus, etc.)

### 🔄 Backup Verification

- [ ] Check backup created: `ls -la postgres-backups/`
- [ ] Test backup restoration:
  ```bash
  # Create test database
  docker-compose exec postgres createdb -U airflow test_restore
  
  # Restore backup (if needed)
  ```

---

## Post-Deployment Checklist

### 🔐 Security Hardening

- [ ] Update default Airflow password:
  - [ ] Login to UI
  - [ ] Go to Settings → Security → Passwords
  - [ ] Change airflow user password
- [ ] Create additional users/roles as needed
- [ ] Configure LDAP/Active Directory integration (if needed)
- [ ] Enable API authentication
- [ ] Set up audit logging

### 📚 Documentation

- [ ] Document custom DAGs location
- [ ] Document backup/restore procedures
- [ ] Document scaling procedures
- [ ] Document troubleshooting steps
- [ ] Create runbook for operations team

### 👥 Team Training

- [ ] Train operators on:
  - [ ] How to view logs
  - [ ] How to restart services
  - [ ] How to update DAGs
  - [ ] How to scale services
  - [ ] Backup and recovery procedures
- [ ] Assign on-call rotation
- [ ] Set up escalation procedure

### 📊 Monitoring & Alerts

- [ ] CPU utilization alerts (> 80%)
- [ ] Memory utilization alerts (> 85%)
- [ ] Disk space alerts (< 10%)
- [ ] Service down alerts
- [ ] DAG failure alerts
- [ ] SLA violation alerts

### 🔄 Maintenance Planning

- [ ] Schedule regular backups
- [ ] Schedule security updates
- [ ] Schedule performance optimization reviews
- [ ] Schedule security audits (quarterly)
- [ ] Schedule disaster recovery drills (semi-annual)

---

## Troubleshooting Checklist

### 🔧 If Services Don't Start

- [ ] Check Docker/Docker Compose version
- [ ] Check available disk space: `df -h`
- [ ] Check available memory: `free -h`
- [ ] Check Docker logs: `docker-compose logs`
- [ ] Check specific service: `docker-compose logs [service]`
- [ ] Rebuild images: `docker-compose build --no-cache`

### 🔗 If Can't Connect to Database

- [ ] Check PostgreSQL is running: `docker-compose ps postgres`
- [ ] Check connection string in `.env`
- [ ] Test connection:
  ```bash
  docker-compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}
  ```
- [ ] Check network connectivity between containers
- [ ] Check password is correct in `.env`

### 🔴 If Redis Connection Fails

- [ ] Check Redis is running: `docker-compose ps redis`
- [ ] Test Redis:
  ```bash
  docker-compose exec redis redis-cli -a ${REDIS_PASSWORD} ping
  ```
- [ ] Check password in `.env` matches docker-compose.yml
- [ ] Check Redis logs: `docker-compose logs redis`

### 🔓 If Kerberos Authentication Fails

- [ ] Check Kerberos config: `docker-compose exec airflow-scheduler cat /etc/krb5.conf`
- [ ] Test Kerberos:
  ```bash
  docker-compose exec airflow-scheduler kinit -V user@OKCO.IR
  ```
- [ ] Check KDC connectivity:
  ```bash
  docker-compose exec airflow-scheduler nslookup okdc10003.okco.ir
  ```
- [ ] Check system time sync (important for Kerberos)

### 🎯 If UI Not Accessible

- [ ] Check port 9280 is not in use:
  ```bash
  netstat -an | grep 9280
  ```
- [ ] Check firewall rules
- [ ] Check Airflow API server logs: `docker-compose logs airflow-apiserver`
- [ ] Check service is healthy: `docker-compose ps airflow-apiserver`

---

## Security Incident Response

### If Credentials Are Compromised

- [ ] Rotate `.env` file immediately
- [ ] Re-generate all secrets:
  - [ ] FERNET_KEY
  - [ ] JWT_SECRET
  - [ ] WEBSERVER_SECRET_KEY
  - [ ] POSTGRES_PASSWORD
  - [ ] REDIS_PASSWORD
- [ ] Restart all services: `docker-compose restart`
- [ ] Review logs for suspicious activity
- [ ] Change Airflow user passwords
- [ ] Notify security team

### If Unauthorized Access Is Detected

- [ ] Shut down services immediately: `docker-compose down`
- [ ] Preserve logs for forensics
- [ ] Review Docker logs: `docker-compose logs > forensics.log`
- [ ] Check database backup integrity
- [ ] Notify security team
- [ ] Perform security audit
- [ ] Implement additional controls before restart

---

**Last Updated:** 2026-05-24  
**Version:** 1.0  
**Owner:** DevOps Team
