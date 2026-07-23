# Airflow 3.2.1 Windows Authentication Build

این پوشه برای ساخت یک ایمیج سفارشی Airflow 3.2.1 با پشتیبانی از **Kerberos / Windows Authentication** و اتصال به **SQL Server**، **Kafka**، **MySQL** و **PostgreSQL** آماده شده است.

## فایل‌های موجود

- `Dockerfile.winauth.3.2.1`
- `docker-compose.winaut.build.yml`
- `config/`

## هدف این پوشه

این زیرپوشه صرفاً ابزار ساخت ایمیج Airflow را دارد.‌ خود `docker-compose.winaut.build.yml` فقط سرویس `app` را تعریف می‌کند و استک کامل پایگاه داده یا بروکری را ندارد.

## آنچه نصب می‌شود

- Apache Airflow 3.2.1
- Kerberos client و پشتیبانی Windows Auth
- Microsoft ODBC Driver 18 برای SQL Server
- بسته‌های Python: `pyodbc`, `pymssql`, `apache-airflow-providers-microsoft-mssql`, `apache-airflow-providers-apache-kafka`, `confluent-kafka`, `avro-python3`, `fastavro`, `clickhouse-driver`, `pymongo`, `mysqlclient`, `apache-airflow-providers-mysql`, `apache-airflow-providers-postgres`, `psycopg2-binary`, `apache-airflow-providers-hashicorp`, `apache-airflow-providers-http`

## نحوه استفاده

1. `config/krb5.conf` را بررسی و طبق دامنه خود ویرایش کنید.
2. ایمیج را بسازید:

```powershell
docker compose -f docker-compose.winaut.build.yml build
```

3. برای اجرای سرویس، باید یک فایل `docker-compose.yml` پایه نیز در دسترس داشته باشید. مثال:

```powershell
docker compose -f docker-compose.yml -f docker-compose.winaut.build.yml up -d
```

## نکات مهم

- بدون فایل پایه، این فولدر به تنهایی استک کامل را اجرا نمی‌کند.
- نام ایمیج پیش‌فرض: `airflow-data-platform-winauth:3.2.1`
- زمان اجرای Airflow با timezone `Asia/Tehran` پیکربندی شده است.
