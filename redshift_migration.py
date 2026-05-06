import boto3
import time
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import os

# ================== CONFIGURATION ==================
BUCKET = "vaani-bq-data-dump"
PARENT_PREFIX = ""
REDSHIFT_WORKGROUP = "prod-vaaniresearch-redshift-workgroup"
REDSHIFT_DATABASE = "vaani-research-tts"
REDSHIFT_SCHEMA = "tts_dataset_devel"

REGION = "us-east-1"

IAM_ROLE_ARN = "arn:aws:iam::504804196173:role/service-role/AmazonRedshift-CommandsAccessRole-20260417T155528"

FILTER_TABLES = []
# =================================================

s3_client = boto3.client("s3", region_name=REGION)
redshift_data = boto3.client("redshift-data", region_name=REGION)


def execute_sql(sql: str, statement_name: str = None):
    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
        StatementName=statement_name
    )
    query_id = response["Id"]
    print(f"Started: {statement_name} (ID: {query_id})")

    while True:
        status = redshift_data.describe_statement(Id=query_id)
        state = status["Status"]

        if state in ["FINISHED", "FAILED", "ABORTED"]:
            if state == "FINISHED":
                print(f"Completed: {statement_name}")
                return
            else:
                raise Exception(status.get("Error"))

        time.sleep(2)


def arrow_to_redshift(dtype: pa.DataType) -> str:
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    elif pa.types.is_int64(dtype):
        return "BIGINT"
    elif pa.types.is_int32(dtype):
        return "INTEGER"
    elif pa.types.is_float64(dtype):
        return "DOUBLE PRECISION"
    elif pa.types.is_string(dtype):
        return "VARCHAR(65535)"
    elif isinstance(dtype, pa.TimestampType):
        return "TIMESTAMP"
    elif pa.types.is_decimal(dtype):
        return f"DECIMAL({min(dtype.precision,38)},{dtype.scale})"
    elif pa.types.is_list(dtype) or pa.types.is_struct(dtype):
        return "SUPER"
    else:
        return "VARCHAR(65535)"


def migrate_table(table_prefix: str, table_name_raw: str):
    table_name = table_name_raw.replace('"', '').strip()

    print(f'Processing table: "{table_name}"')

    paginator = s3_client.get_paginator("list_objects_v2")

    parquet_key = None
    for page in paginator.paginate(Bucket=BUCKET, Prefix=table_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                parquet_key = obj["Key"]
                break
        if parquet_key:
            break

    if not parquet_key:
        print("No parquet found, skipping")
        return

    print(f"Using file: {parquet_key}")

    # ✅ Download one file locally
    tmp = tempfile.NamedTemporaryFile(delete=False)
    local_path = tmp.name
    tmp.close()

    s3_client.download_file(BUCKET, parquet_key, local_path)

    schema = pq.read_schema(local_path)

    os.remove(local_path)

    columns = []
    for field in schema:
        col = field.name.replace('"', '').strip()
        columns.append(f'"{col}" {arrow_to_redshift(field.type)}')

    create_sql = f"""
    DROP TABLE IF EXISTS "{REDSHIFT_SCHEMA}"."{table_name}";
    CREATE TABLE "{REDSHIFT_SCHEMA}"."{table_name}" (
        {", ".join(columns)}
    );
    """

    execute_sql(create_sql, f"CREATE {table_name}")

    copy_sql = f"""
    COPY "{REDSHIFT_SCHEMA}"."{table_name}"
    FROM 's3://{BUCKET}/{table_prefix}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS PARQUET;
    """

    execute_sql(copy_sql, f"COPY {table_name}")

    print(f'Finished table "{table_name}"\n')


def main():
    print("Discovering tables...")

    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PARENT_PREFIX, Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            prefix = prefix_info["Prefix"]
            table_name = prefix.rstrip("/").split("/")[-1]

            if FILTER_TABLES and table_name.lower() not in [t.lower() for t in FILTER_TABLES]:
                continue

            migrate_table(prefix, table_name)


if __name__ == "__main__":
    main()