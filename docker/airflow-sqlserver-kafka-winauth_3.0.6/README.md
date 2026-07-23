# Airflow SQL Server + Kafka + Windows Auth

این پوشه یک استک Airflow با Windows Authentication، PostgreSQL، Redis و سرویس‌های Airflow را شامل می‌شود.

## فایل‌های موجود

- `docker-compose.winauth.yml` – استک کامل سرویس‌های Airflow
- `docker-compose.winaut.build.yml` – overlay برای ساخت ایمیج سفارشی از `Dockerfile.winauth`
- `Dockerfile.winauth`
- `config/`

## ویژگی‌ها

- Airflow 3.0.6
- پشتیبانی Kerberos / Windows Authentication
- SQL Server با ODBC و providers مایکروسافت
- Kafka integration
- بک‌اند metadata PostgreSQL
- بروکر Redis

## نحوه اجرا

### حالت ساده

```powershell
docker compose -f docker-compose.winauth.yml up -d
```

### حالت build سفارشی

1. ایمیج را بسازید:

```powershell
docker compose -f docker-compose.winaut.build.yml build
```

2. اگر `docker-compose.winaut.build.yml` را می‌خواهید در کنار استک کامل اجرا کنید، باید آن را با یک فایل پایه ترکیب کنید.

## سرویس‌های کلیدی

- `postgres` – پایگاه داده metadata
- `redis` – Celery broker
- `airflow-apiserver` – UI و API
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `airflow-init`

## پورت‌های پیش‌فرض

- Airflow UI / API: `9280`
- PostgreSQL: `9432` (host)

## پیکربندی Kerberos

- `config/krb5.conf` را با تنظیمات دامنه خود ویرایش کنید.
- `AIRFLOW_PID` را در صورت نیاز تنظیم کنید.

## یادداشت

- `docker-compose.winauth.yml` خدمات اصلی را دارد و این فولدر برای اجرا مناسب‌ترین گزینه است.
- `docker-compose.winaut.build.yml` تنها ایمیج سفارشی را تعریف می‌کند.
