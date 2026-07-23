# Airflow 3.0.6 Windows Authentication Build

این پوشه برای ساخت یک ایمیج سفارشی Airflow 3.0.6 با پشتیبانی از **Kerberos / Windows Authentication** و اتصال به **SQL Server**، **Kafka**، **MySQL** و **PostgreSQL** آماده شده است.

## فایل‌های موجود

- `Dockerfile.winauth.3.0.6`
- `docker-compose.winaut.build.yml`
- `config/`

## هدف این پوشه

این پوشه فقط فایل‌های مورد نیاز برای ساخت و اجرای ایمیج سفارشی را دارد. فایلی که یک استک کامل services مانند `postgres` و `redis` را تعریف کند، در این زیرپوشه وجود ندارد.

## آنچه نصب می‌شود

- Apache Airflow 3.0.6
- Kerberos client و پشتیبانی Windows Auth
- Microsoft ODBC Driver 18 برای SQL Server
- بسته‌های Python: `pyodbc`, `pymssql`, `apache-airflow-providers-microsoft-mssql`, `apache-airflow-providers-apache-kafka`, `confluent-kafka`, `avro-python3`, `fastavro`, `clickhouse-driver`, `mysqlclient`, `apache-airflow-providers-mysql`, `apache-airflow-providers-postgres`, `apache-airflow-providers-hashicorp`, `apache-airflow-providers-http`

## نحوه استفاده

1. ابتدا `config/krb5.conf` را با تنظیمات دامنه و KDC خود ویرایش کنید.
2. ایمیج را بسازید:

```powershell
docker compose -f docker-compose.winaut.build.yml build
```

3. اگر می‌خواهید همین فولدر را اجرا کنید، توجه داشته باشید که این فایل تنها سرویس `app` را تعریف می‌کند. برای اجرای کامل، باید یک فایل `docker-compose.yml` پایه با سرویس‌های `postgres` و `redis` نیز داشته باشید.

مثال با فایل پایه:

```powershell
docker compose -f docker-compose.yml -f docker-compose.winaut.build.yml up -d
```

## نکات مهم

- اگر فقط از `docker-compose.winaut.build.yml` استفاده کنید، فقط کانتینر Airflow ساخته خواهد شد و به سرویس‌های پشتیبانی نیاز دارد.
- نام ایمیج پیش‌فرض: `airflow-data-platform-winauth:3.0.6`
- اگر `AIRFLOW_UID` را تنظیم نکنید، کاربر داخل کانتینر به شکل پیش‌فرض `50000` خواهد بود.

## مسیرهای مهم

- `config/krb5.conf` – پیکربندی Kerberos
- `Dockerfile.winauth.3.0.6` – تعریف image
- `docker-compose.winaut.build.yml` – definition میزبانی سرویس `app`
