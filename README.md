# 📦 S3 Parquet to Amazon Redshift Migration Script

This Python script automates the process of discovering Parquet datasets in an S3 bucket and loading them into Amazon Redshift Serverless tables.

It dynamically:
- Infers schema from Parquet files
- Creates Redshift tables
- Loads data using the `COPY` command

---

## 🚀 Features

- 🔍 Auto-discovers tables from S3 prefixes
- 🧠 Infers schema using PyArrow
- 🏗️ Creates Redshift tables dynamically
- 📥 Loads data using optimized Parquet COPY
- 🔁 Supports filtering specific tables
- ⏳ Waits for query completion with status tracking

---

## 🧰 Tech Stack

- Python 3.x
- boto3 (AWS SDK)
- PyArrow (Parquet schema handling)
- Amazon S3
- Amazon Redshift Serverless

---

## ⚙️ Configuration

Update the following variables in the script before running:

```python
BUCKET = "your-s3-bucket"
PARENT_PREFIX = ""  # Root prefix containing table folders

REDSHIFT_WORKGROUP = "your-redshift-workgroup"
REDSHIFT_DATABASE = "your-database"
REDSHIFT_SCHEMA = "your-schema"

REGION = "us-east-1"

IAM_ROLE_ARN = "your-redshift-iam-role"

FILTER_TABLES = []  # Optional: ["table1", "table2"]
```

---

## 📁 Expected S3 Structure

```
s3://<BUCKET>/<PARENT_PREFIX>/<table_name>/file1.parquet
s3://<BUCKET>/<PARENT_PREFIX>/<table_name>/file2.parquet
```

Each folder inside `PARENT_PREFIX` is treated as a separate table.

---

## 🧠 How It Works

### 1. Discover Tables
- Lists S3 prefixes under `PARENT_PREFIX`
- Each prefix is treated as a table

### 2. Schema Extraction
- Downloads one `.parquet` file per table
- Uses PyArrow to extract schema

### 3. Type Mapping

| PyArrow Type     | Redshift Type       |
|------------------|---------------------|
| boolean          | BOOLEAN             |
| int64            | BIGINT              |
| int32            | INTEGER             |
| float64          | DOUBLE PRECISION    |
| string           | VARCHAR(65535)      |
| timestamp        | TIMESTAMP           |
| decimal          | DECIMAL(p,s)        |
| list / struct    | SUPER               |
| other            | VARCHAR(65535)      |

### 4. Table Creation

```sql
DROP TABLE IF EXISTS schema.table;

CREATE TABLE schema.table (
    column definitions...
);
```

### 5. Data Load

```sql
COPY schema.table
FROM 's3://bucket/prefix/'
IAM_ROLE 'your-role'
FORMAT AS PARQUET;
```

---

## ▶️ How to Run

### 1. Prerequisites

- Python 3.8+
- AWS account with access to:
  - S3 bucket
  - Redshift Serverless
- IAM Role with required permissions

---

### 2. Install Dependencies

```bash
pip install boto3 pyarrow
```

---

### 3. Configure AWS Credentials

Option A: Using AWS CLI

```bash
aws configure
```

Option B: Environment variables

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

---

### 4. Update Script Configuration

Edit the script and set:
- S3 bucket
- Redshift workgroup/database/schema
- IAM role ARN

---

### 5. Run the Script

```bash
python script.py
```

---

## 🔐 IAM Permissions Required

### S3 Access

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": "*"
}
```

### Redshift Data API

```json
{
  "Effect": "Allow",
  "Action": [
    "redshift-data:ExecuteStatement",
    "redshift-data:DescribeStatement"
  ],
  "Resource": "*"
}
```

---

## 🧪 Optional: Filter Specific Tables

```python
FILTER_TABLES = ["users", "transactions"]
```

Only these tables will be processed.

---

## ⚠️ Notes & Limitations

- Only **one Parquet file** is used for schema inference
- Existing tables are **dropped and recreated**
- No incremental loads (full reload every run)
- Assumes consistent schema across all Parquet files in a folder
- Large VARCHAR default (`65535`) may not be optimal

---

## 🛠️ Possible Improvements

- Schema evolution support
- Incremental/delta loads
- Partition awareness
- Parallel table processing
- Logging & retry mechanisms
- Column compression encoding

---

## 🐞 Troubleshooting

### ❌ No Parquet Found
- Ensure files end with `.parquet`
- Check S3 prefix correctness

### ❌ COPY Fails
- Validate IAM role permissions
- Check Redshift access to S3
- Ensure file format is valid Parquet

### ❌ Query Stuck
- Script polls every 2 seconds
- Check Redshift console for query status

---

## 📄 License

MIT License (or update as per your organization)

