# Airflow SQL Server + Kafka + Timezone

این پوشه یک overlay برای ساخت ایمیج Airflow با timezone `Asia/Tehran` است. این فولدر خود یک استک کامل Docker Compose ندارد و تنها `app` را با Dockerfile دلخواه تعریف می‌کند.

## فایل‌های موجود

- `docker-compose.winaut.timezone.build.yml`
- `Dockerfile.winauth.timezone`
- `config/`

## هدف این پوشه

ساخت ایمیج سفارشی بر اساس `airflow-sqlserver-kafka-winauth:3.0.6` و افزودن تنظیمات timezone.

## نحوه استفاده

1. `config/krb5.conf` را بررسی و اصلاح کنید.
2. ایمیج را بسازید:

```powershell
docker compose -f docker-compose.winaut.timezone.build.yml build
```

3. این فایل به تنهایی سرویس پایگاه داده یا Redis را تعریف نمی‌کند. برای اجرای کامل، باید آن را با یک فایل `docker-compose.yml` پایه دیگر ترکیب کنید.

## نکات مهم

- زمان کانتینر: `Asia/Tehran`
- ایمیج ساخته شده: `airflow-sqlserver-kafka-winauth-timezone:3.0.6`
