# Docker Overview - Airflow Deployments

این پوشه شامل نسخه‌های مختلف تنظیمات Docker برای راه‌اندازی **Apache Airflow** است. هر زیرپوشه مستقل است و فایل‌های `docker-compose`، `Dockerfile` و `config/` مربوط به همان نسخه را در خود دارد.

> توجه: در این پوشه `docker-compose.yml` سطح بالایی وجود ندارد. برای اجرا باید به زیرپوشه مورد نظر بروید یا مسیر فایل compose را مشخص کنید.

---

## 📁 ساختار پوشه‌ها

| پوشه | توضیح | فایل‌های اصلی |
|------|-------|---------------|
| `airflow-data-platform-winauth_3.0.6/` | Airflow 3.0.6 با Windows Authentication برای دیتاپلتفرم | `docker-compose.winaut.build.yml`, `Dockerfile.winauth.3.0.6`, `config/` |
| `airflow-data-platform-winauth_3.2.1/` | Airflow 3.2.1 با Windows Authentication | `docker-compose.winaut.build.yml`, `Dockerfile.winauth.3.2.1`, `config/` |
| `airflow-data-platform-winauth_3.2.1_harden/` | نسخه Hardened Airflow 3.2.1 با Windows Authentication | `docker-compose.winauth.yml`, `docker-compose.prod.yml`, `.env.example`, `README.md`, `SECURITY_GUIDE.md`, `DEPLOYMENT_CHECKLIST.md`, `setup-security.sh`, `setup-security.bat`, `config/` |
| `airflow-sqlserver-kafka-winauth_3.0.6/` | Airflow 3.0.6 با Kafka و SQL Server Windows Auth | `docker-compose.winauth.yml`, `docker-compose.winaut.build.yml`, `Dockerfile.winauth`, `config/` |
| `airflow-sqlserver-kafka-winauth-timezone_3.0.6/` | Airflow 3.0.6 با Kafka، SQL Server و پشتیبانی timezone | `docker-compose.winaut.timezone.build.yml`, `Dockerfile.winauth.timezone`, `config/` |
| `airflow-sqlserver-kafka-winauth-timezone-mysql_3.0.6/` | Airflow 3.0.6 با Kafka، SQL Server، MySQL و timezone | `docker-compose.winaut.timezone.mysql.build.yml`, `Dockerfile.winauth.timezone.mysql`, `config/` |
| `README.md` | این فایل راهنما |

---

## 🔎 نحوه استفاده

1. به زیرپوشه‌ای که با نیاز شما مطابقت دارد بروید.
2. فایل `docker-compose` مورد نظر را بررسی کنید.
3. متغیرهای محیطی را از `.env.example` (در صورت وجود) در همان پوشه تکمیل کنید.
4. دستور `docker compose` را با استفاده از فایل Compose داخل همان زیرپوشه اجرا کنید.

مثال:

```bash
cd docker/airflow-data-platform-winauth_3.2.1_harden
cp .env.example .env
# سپس طبق README محلی آن پوشه اجرا کنید
docker compose -f docker-compose.winauth.yml up -d
```

---

## ✅ مرور سریع زیرپوشه‌ها

### `airflow-data-platform-winauth_3.0.6/`
- build compose برای Airflow 3.0.6 با Windows Authentication

### `airflow-data-platform-winauth_3.2.1/`
- build compose برای Airflow 3.2.1 با Windows Authentication

### `airflow-data-platform-winauth_3.2.1_harden/`
- نسخه Hardened Airflow 3.2.1
- حاوی `.env.example`, `README.md`, `SECURITY_GUIDE.md`, `DEPLOYMENT_CHECKLIST.md`

### `airflow-sqlserver-kafka-winauth_3.0.6/`
- Airflow 3.0.6 با Kafka و SQL Server Windows Authentication

### `airflow-sqlserver-kafka-winauth-timezone_3.0.6/`
- مشابه مورد قبل با پشتیبانی timezone

### `airflow-sqlserver-kafka-winauth-timezone-mysql_3.0.6/`
- سناریوی Kafka، SQL Server و MySQL همراه با timezone

---

## 📌 نکته مهم

- هر زیرپوشه معمولاً `config/` دارد، ولی ساختار داخلی هر کدام متفاوت است.
- اگر README محلی در زیرپوشه وجود داشت، حتماً آن را برای جزئیات دقیق‌تر بخوانید.
- این README یک فهرست شاخص است و دستورهای دقیق اجرایی را اغلب باید در README مخصوص هر زیرپوشه دنبال کنید.

---

## 📚 پیشنهاد

برای شروع با بهترین پشتیبانی مستندات و امنیت، به `docker/airflow-data-platform-winauth_3.2.1_harden/README.md` مراجعه کنید.
