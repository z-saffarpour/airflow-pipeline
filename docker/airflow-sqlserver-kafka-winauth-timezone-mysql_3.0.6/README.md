# Airflow SQL Server + Kafka + Timezone + MySQL

این پوشه یک overlay برای ساخت ایمیج Airflow با پشتیبانی MySQL و timezone `Asia/Tehran` است. این فولدر به خودی خود استک کامل Docker Compose را شامل نمی‌شود.

## فایل‌های موجود

- `docker-compose.winaut.timezone.mysql.build.yml`
- `Dockerfile.winauth.timezone.mysql`
- `config/`

## هدف این پوشه

ساخت ایمیج سفارشی بر اساس `airflow-sqlserver-kafka-winauth:3.0.6` و افزودن:
- timezone `Asia/Tehran`
- `mysqlclient`
- `apache-airflow-providers-mysql`
- `apache-airflow-providers-common-sql`
- `apache-airflow-providers-http`
- `apache-airflow-providers-postgres`
- `psycopg2-binary`
- `pymongo`

## نحوه استفاده

1. `config/krb5.conf` را بررسی و اصلاح کنید.
2. ایمیج را بسازید:

```powershell
docker compose -f docker-compose.winaut.timezone.mysql.build.yml build
```

3. برای اجرا باید این overlay را با یک فایل `docker-compose.yml` پایه یا استک کامل دیگر ترکیب کنید.

## نکات مهم

- نام ایمیج ساخته شده: `airflow-sqlserver-kafka-winauth-timezone-mysql:3.0.6`
- این پوشه شامل سرویس‌های `postgres` یا `redis` نیست.
