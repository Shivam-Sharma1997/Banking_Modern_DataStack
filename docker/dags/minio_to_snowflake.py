import os
from pathlib import Path

import boto3
import snowflake.connector
from airflow.decorators import dag, task
from pendulum import datetime


TABLES = ("customers", "accounts", "transactions")


def get_env(name, default=None):
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dag(
    dag_id="minio_to_snowflake_banking",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["banking", "minio", "snowflake"],
)
def minio_to_snowflake_banking():

    @task
    def download_from_minio():
        minio_endpoint = get_env("MINIO_ENDPOINT")
        minio_access_key = get_env("MINIO_ACCESS_KEY")
        minio_secret_key = get_env("MINIO_SECRET_KEY")
        minio_bucket = get_env("MINIO_BUCKET")
        local_dir = Path(os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads"))

        local_dir.mkdir(parents=True, exist_ok=True)

        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )

        downloaded_files = {}

        for table in TABLES:
            prefix = f"{table}/"
            response = s3.list_objects_v2(Bucket=minio_bucket, Prefix=prefix)
            objects = response.get("Contents", [])

            table_files = []
            for obj in objects:
                key = obj["Key"]

                if key.endswith("/"):
                    continue

                local_path = local_dir / f"{table}_{Path(key).name}"
                s3.download_file(minio_bucket, key, str(local_path))
                print(f"Downloaded {key} -> {local_path}")
                table_files.append(str(local_path))

            downloaded_files[table] = table_files

        return downloaded_files

    @task
    def load_to_snowflake(local_files):
        if not local_files:
            print("No files found to load.")
            return

        conn_kwargs = {
            "user": get_env("SNOWFLAKE_USER"),
            "password": get_env("SNOWFLAKE_PASSWORD"),
            "account": get_env("SNOWFLAKE_ACCOUNT"),
            "warehouse": get_env("SNOWFLAKE_WAREHOUSE"),
            "database": get_env("SNOWFLAKE_DB"),
            "schema": get_env("SNOWFLAKE_RAW_SCHEMA", get_env("SNOWFLAKE_SCHEMA")),
        }

        snowflake_role = os.getenv("SNOWFLAKE_ROLE")
        if snowflake_role:
            conn_kwargs["role"] = snowflake_role

        with snowflake.connector.connect(**conn_kwargs) as conn:
            with conn.cursor() as cur:
                for table, files in local_files.items():
                    if not files:
                        print(f"No files for {table}, skipping.")
                        continue

                    for file_path in files:
                        put_sql = f"PUT file://{file_path} @%{table} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                        cur.execute(put_sql)
                        print(f"Uploaded {file_path} -> @{table}")

                    copy_sql = f"""
                    COPY INTO {table} (v)
                    FROM (SELECT $1 FROM @%{table})
                    FILE_FORMAT = (TYPE = PARQUET)
                    ON_ERROR = 'CONTINUE'
                    """
                    cur.execute(copy_sql)
                    print(f"Loaded data into {table}")

            conn.commit()

    load_to_snowflake(download_from_minio())


dag = minio_to_snowflake_banking()