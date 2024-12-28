# s3tables-locally
s3tables-locally


# steps 

### step1 : 
```
export AWS_ACCESS_KEY_ID="XX"
export AWS_SECRET_ACCESS_KEY="XX"
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
pip3 install pyspark==3.4.0
```

### step 2: copy files to S3 
```
# INserts
aws s3 cp /Users/soumilshah/IdeaProjects/emr-labs/tmp/datafiles/initialsinserts/e37d8fae-51e2-4716-bfb1-7381d7e58bcf.csv  s3://<BUCKET>/raw/e37d8fae-51e2-4716-bfb1-7381d7e58bcf.csv

# Updates
aws s3 cp /Users/soumilshah/IdeaProjects/emr-labs/tmp/datafiles/initialsinserts/6a45a9d1-9ef4-4df1-99d6-2d1add3b2e94.csv  s3://<BUCKET>/raw/6a45a9d1-9ef4-4df1-99d6-2d1add3b2e94.csv

```

### step 3: Define your Schema Config and MergeSQL files

# Schema file
```
{
  "type": "record",
  "name": "InvoiceRecord",
  "fields": [
    {
      "name": "invoiceid",
      "type": "int",
      "default": 0
    },
    {
      "name": "itemid",
      "type": "int",
      "default": 0
    },
    {
      "name": "category",
      "type": "string",
      "default": ""
    },
    {
      "name": "price",
      "type": "float",
      "default": 0.0
    },
    {
      "name": "quantity",
      "type": "int",
      "default": 1
    },
    {
      "name": "orderdate",
      "type": "string",
      "default": ""
    },
    {
      "name": "destinationstate",
      "type": "string",
      "default": ""
    },
    {
      "name": "shippingtype",
      "type": "string",
      "default": ""
    },
    {
      "name": "referral",
      "type": "string",
      "default": ""
    }

  ]
}

```

# MergeSQL
```
MERGE INTO s3tablesbucket.example_namespace.orders AS target
    USING (
        SELECT
            invoiceid,
            itemid,
            category,
            price,
            quantity,
            orderdate,
            destinationstate,
            shippingtype,
            referral
        FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (
                   PARTITION BY invoiceid, itemid
                   ORDER BY replicadmstimestamp DESC
               ) AS row_num
                 FROM source_table
             ) AS deduped_source
        WHERE row_num = 1
    ) AS source
    ON target.invoiceid = source.invoiceid AND target.itemid = source.itemid
    WHEN MATCHED THEN
        UPDATE SET
            target.category = source.category,
            target.price = source.price,
            target.quantity = source.quantity,
            target.orderdate = source.orderdate,
            target.destinationstate = source.destinationstate,
            target.shippingtype = source.shippingtype,
            target.referral = source.referral
    WHEN NOT MATCHED THEN
        INSERT (
                invoiceid, itemid, category, price, quantity, orderdate, destinationstate, shippingtype, referral
            )
            VALUES (
                       source.invoiceid, source.itemid,
                       source.category, source.price,
                       source.quantity, source.orderdate,
                       source.destinationstate, source.shippingtype,
                       source.referral
                   );

```

# Config file
```
{
  "spark": {
    "spark.app.name": "iceberg_lab",
    "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.defaultCatalog": "s3tablesbucket",
    "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.sql.catalog.s3tablesbucket.warehouse": "XXXXXX",
    "spark.sql.catalog.s3tablesbucket.client.region": "us-east-1",
    "spark.sql.catalog.dev.s3.endpoint": "https://s3.amazonaws.com",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  },
  "input_config": {
    "protocol": "s3a",
    "commit_checkpoint": true,
    "path": "s3a://<BUCKET>/raw/",
    "type": "csv",
    "transform_query": "",
    "mode": "inc",
    "checkpoint_path": "s3a://<BUCKET>/checkpoint/orders.json",
    "csv_options": {
      "sep": "\t",
      "header": "true",
      "inferSchema": "true"
    }
  },
  "output_config": {
    "catalog_name": "s3tablesbucket",
    "database": "example_namespace",
    "table_name": "orders",
    "type": "table_buckets",
    "mode": "merge",
    "schema": "/Users/soumilshah/IdeaProjects/emr-labs/e6data/scripts/BatchJobs/RawSilver/schema/silver_orders.avsc",
    "merge_query": "/Users/soumilshah/IdeaProjects/emr-labs/e6data/scripts/BatchJobs/RawSilver/SQL/RawToSilverMergeSql.sql",
    "table_type": "COW",
    "compression": "zstd",
    "partition": "destinationstate"
  }
}

```

# Fire job

```
python3 Template.py <PATH>/RawToSilver.json

```
