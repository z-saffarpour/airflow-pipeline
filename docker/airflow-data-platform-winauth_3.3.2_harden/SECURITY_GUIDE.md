# =============================================================================
# Security Configuration Guide for Airflow + SQL Server + Kafka Pipeline
# =============================================================================

## 1. INITIAL SETUP - REQUIRED STEPS

### Step 1.1: Create .env file
```bash
cp .env.example .env
```

### Step 1.2: Generate Secure Keys (Run in Python)
```python
from cryptography.fernet import Fernet
import os

# Generate Fernet Key
fernet_key = Fernet.generate_key().decode()
print(f"AIRFLOW__CORE__FERNET_KEY={fernet_key}")

# Generate JWT Secret
jwt_secret = os.urandom(32).hex()
print(f"AIRFLOW__API_AUTH__JWT_SECRET={jwt_secret}")

# Generate Webserver Secret
webserver_secret = os.urandom(32).hex()
print(f"AIRFLOW__WEBSERVER__SECRET_KEY={webserver_secret}")
```

### Step 1.3: Update .env file with generated keys
Edit `.env` file and replace all `ChangeMe_*` values with:
- AIRFLOW__CORE__FERNET_KEY: Generated Fernet key
- AIRFLOW__API_AUTH__JWT_SECRET: Generated JWT secret
- AIRFLOW__WEBSERVER__SECRET_KEY: Generated Webserver secret
- POSTGRES_PASSWORD: Strong password (min 16 chars)
- REDIS_PASSWORD: Strong password (min 16 chars)

Example strong password generation:
```bash
openssl rand -base64 32
```

### Step 1.4: Set Secure File Permissions
```bash
# Protect .env file
chmod 600 .env

# Secure config directory
chmod 700 config/
chmod 600 config/krb5.conf
chmod 600 config/airflow.cfg
```

### Step 1.5: Update Kerberos Configuration
Edit `config/krb5.conf` and verify:
- Realm is set to OKCO.IR
- KDC is correctly configured
- Domain controller is accessible

---

## 2. DATABASE SECURITY

### PostgreSQL
- ✓ Uses strong password from .env
- ✓ Configured with performance settings
- ✓ Health checks enabled
- ✓ Backup service configured

**Recommendations:**
- Change default user from `airflow` to a custom user:
  ```
  POSTGRES_USER=airflow_prod
  ```
- Enable SSL connections (production):
  ```
  ssl=on
  ```

### Redis
- ✓ Now requires password authentication
- ✓ Max memory policy set to `allkeys-lru`
- ✓ AOF persistence enabled

**Password protection:**
```
requirepass ${REDIS_PASSWORD}
```

---

## 3. AIRFLOW SECURITY

### Authentication & Authorization
- ✓ FAB (Flask App Builder) Auth Manager enabled
- ✓ RBAC enabled
- ✓ JWT authentication configured
- ✓ Webserver secret key randomized

### API Security
- ✓ Config exposure disabled (AIRFLOW__WEBSERVER__EXPOSE_CONFIG: false)
- ✓ Secure cookies enabled
- ✓ SameSite cookie policy: Lax
- ✓ Session timeout: 3600 seconds

### Encryption
- ✓ Fernet key for sensitive data encryption
- ✓ Generated from secure random source

---

## 4. KERBEROS / WINDOWS AUTHENTICATION

### Configuration File
Located: `config/krb5.conf`

**Current Settings:**
- Realm: OKCO.IR
- KDC: okdc10003.okco.ir
- Ticket Lifetime: 24 hours
- Renew Lifetime: 7 days

**For Production:**
1. Ensure KDC is accessible from containers
2. Configure DNS resolution properly
3. Enable Kerberos tracing (optional):
   ```
   KRB5_TRACE=/var/log/krb5_trace.log
   ```

---

## 5. ENVIRONMENT VARIABLES

All sensitive data is configured via `.env`:
```
✓ Database credentials (PostgreSQL)
✓ Redis password
✓ API secrets (JWT, Webserver)
✓ Encryption keys (Fernet)
✓ Kerberos configuration
✓ Mail & SMTP settings (future)
```

**NEVER commit `.env` to version control!**

---

## 6. NETWORK SECURITY

### Docker Network
- All services communicate via `airflow_net` bridge network
- Network is isolated from host by default
- Only exposed ports: 9280 (API), 9432 (PostgreSQL), 9555 (Flower)

### Recommended Setup
For production, consider:
```yaml
networks:
  airflow_net:
    driver: bridge
    driver_opts:
      com.docker.network.bridge.name: airflow_br0
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

---

## 7. RUNNING SECURELY

### Start Services
```bash
# Copy and edit .env file
cp .env.example .env
nano .env  # Edit with strong passwords

# Set correct file permissions
chmod 600 .env

# Start services
docker-compose -f docker-compose.winauth.yml up -d
```

### Verify Setup
```bash
# Check running services
docker-compose -f docker-compose.winauth.yml ps

# Check logs for errors
docker-compose -f docker-compose.winauth.yml logs airflow-apiserver

# Verify Redis password
docker-compose -f docker-compose.winauth.yml exec redis redis-cli -a "${REDIS_PASSWORD}" ping
```

### Access Services
- **Airflow UI**: http://localhost:9280
- **PostgreSQL**: localhost:9432 (for backups)
- **Flower (Celery)**: http://localhost:9555 (if enabled)

Default Airflow user created during init:
- Username: `airflow`
- Password: Set in `_AIRFLOW_WWW_USER_PASSWORD` during first run

---

## 8. SECURITY HARDENING CHECKLIST

### Immediate Actions:
- [ ] Generate and set FERNET_KEY
- [ ] Generate and set JWT_SECRET
- [ ] Generate and set WEBSERVER_SECRET_KEY
- [ ] Set strong POSTGRES_PASSWORD
- [ ] Set strong REDIS_PASSWORD
- [ ] Set file permissions on .env (600)
- [ ] Ensure `.env` is not committed to version control
- [ ] Verify Kerberos KDC connectivity

### Before Production Deployment:
- [ ] Enable TLS for all external connections
- [ ] Configure reverse proxy (nginx/caddy)
- [ ] Set up WAF (Web Application Firewall)
- [ ] Implement API rate limiting
- [ ] Configure logging & monitoring
- [ ] Set up backup strategy
- [ ] Test disaster recovery
- [ ] Perform security audit
- [ ] Implement secrets manager (Vault/AWS Secrets)

### Monitoring & Maintenance:
- [ ] Regular security updates
- [ ] Password rotation policy
- [ ] Backup verification
- [ ] Log analysis
- [ ] Performance monitoring
- [ ] Health check alerts

---

## 9. TROUBLESHOOTING

### Redis Connection Issues
```bash
# Check Redis is running with password
docker-compose exec redis redis-cli -a ${REDIS_PASSWORD} ping

# View Redis logs
docker-compose logs redis
```

### Database Connection Issues
```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

# Check connection string
docker-compose exec airflow-scheduler printenv | grep SQLALCHEMY
```

### Kerberos Issues
```bash
# Check Kerberos configuration
docker-compose exec airflow-scheduler cat /etc/krb5.conf

# Test Kerberos connectivity
docker-compose exec airflow-scheduler kinit -V ${KERBEROS_USER}
```

---

## 10. ADDITIONAL RESOURCES

- [Airflow Security](https://airflow.apache.org/docs/apache-airflow/stable/security-and-api/security/)
- [Kerberos Authentication](https://airflow.apache.org/docs/apache-airflow/stable/security-and-api/kerberos/)
- [Docker Compose Best Practices](https://docs.docker.com/compose/production/)

---

**Last Updated:** 2026-05-24
**Version:** 1.0
**Security Level:** Production Ready
