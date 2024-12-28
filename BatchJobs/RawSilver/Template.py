# ==================================================================
# Standard Imports
# ====================================================================

import os, json, boto3
import json
from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, \
    BooleanType, TimestampType, DateType

from pyspark.sql.avro.functions import from_avro, to_avro


# ==================================================================
# Checkpoint Classes
# ====================================================================

class Checkpoint:
    def __init__(self, checkpoint_path, minio_config=None):
        self.checkpoint_path = checkpoint_path
        self.parsed_url = urlparse(checkpoint_path)
        self.client = self._get_client(minio_config)

    def _get_client(self, minio_config):
        if self.parsed_url.scheme in ['s3', 's3a']:
            if minio_config:
                return boto3.client('s3',
                                    endpoint_url=minio_config['endpoint_url'],
                                    aws_access_key_id=minio_config['access_key'],
                                    aws_secret_access_key=minio_config['secret_key'])
            else:
                return boto3.client('s3')
        return None

    def load(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            try:
                response = self.client.get_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'))
                return json.loads(response['Body'].read().decode('utf-8'))
            except Exception as e:
                print(f"Error loading checkpoint from S3: {e}")
                return None
        else:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f)
            return None

    def save(self, data):
        checkpoint_data = json.dumps(data)
        if self.parsed_url.scheme in ['s3', 's3a']:
            self.client.put_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'),
                                   Body=checkpoint_data)
        else:
            os.makedirs(os.path.dirname(self.checkpoint_path), exist_ok=True)
            with open(self.checkpoint_path, 'w') as f:
                f.write(checkpoint_data)


# ==================================================================
# Abstract Classes
# ====================================================================
class Source(ABC):
    def __init__(self, input_config):
        self.input_config = input_config
        self.path = input_config['path']
        self.mode = input_config.get('mode', 'inc')
        self.checkpoint = Checkpoint(input_config.get('checkpoint_path', 'checkpoint.json'),
                                     input_config.get('minio_config'))

    @abstractmethod
    def read(self, spark):
        pass

    @abstractmethod
    def commit_checkpoint(self):
        pass


class Target(ABC):
    @abstractmethod
    def write(self, df, spark):
        pass


class Schema(ABC):
    @abstractmethod
    def read_schema(self, path, spark):
        pass


# ====================================================================


class FileSource(Source):
    def __init__(self, input_config):
        super().__init__(input_config)
        self.parsed_url = urlparse(self.path)
        self.last_checkpoint_time = self.load_checkpoint()
        self.protocol = input_config.get('protocol', 's3')

    def load_checkpoint(self):
        checkpoint_data = self.checkpoint.load()
        print("checkpoint_data", checkpoint_data)
        return checkpoint_data.get('last_processed_time', 0) if checkpoint_data else 0

    def _list_s3_files(self):
        bucket, prefix = self.parsed_url.netloc, self.parsed_url.path.lstrip('/')
        files = []
        paginator = self.checkpoint.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if self.mode == 'full' or obj['LastModified'].timestamp() > self.last_checkpoint_time:
                    file_path = f"s3://{bucket}/{obj['Key']}"
                    if self.protocol == 's3a':
                        file_path = file_path.replace('s3://', 's3a://')
                    files.append(file_path)
        return files

    def _list_local_files(self):
        files = []
        for root, _, filenames in os.walk(self.parsed_url.path):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                if self.mode == 'full' or os.path.getmtime(file_path) > self.last_checkpoint_time:
                    files.append(file_path)
        return files

    def get_files(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._list_s3_files()
        elif self.parsed_url.scheme == 'file' or not self.parsed_url.scheme:
            return self._list_local_files()
        else:
            raise ValueError(f"Unsupported scheme: {self.parsed_url.scheme}")

    def commit_checkpoint(self):
        current_time = datetime.now().timestamp()
        self.checkpoint.save({'last_processed_time': current_time})
        print(f"Checkpoint updated to: {datetime.fromtimestamp(current_time)}")


# ==================================================================
# Transform Logic
# ====================================================================
class TransformQuery:
    def __init__(self, query):
        self.query = query

    def apply(self, spark, df):
        if not self.query:
            return df

        df.createOrReplaceTempView("temp_view")
        result = spark.sql(self.query)
        spark.catalog.dropTempView("temp_view")
        return result


# ==================================================================
# Source Classes
# ====================================================================

class CsvSource(FileSource, TransformQuery):
    def __init__(self, input_config):
        super().__init__(input_config)

        TransformQuery.__init__(self, query=input_config.get("transform_query", ""))
        self.csv_options = input_config.get('csv_options', {})

    def read(self, spark):
        files = self.get_files()

        if not files:
            print("No files to process")
            return None

        df = spark.read.options(**self.csv_options).csv(files)
        return self.apply(spark, df)


class IcebergSource(Source, Checkpoint, TransformQuery):
    def __init__(self, input_config):
        Checkpoint.__init__(self, checkpoint_path=input_config.get("checkpoint_path"))
        TransformQuery.__init__(self, query=input_config.get("transform_query", ""))
        self.catalog_name = input_config['catalog_name']
        self.database = input_config['database']
        self.table = input_config['table']
        self.mode = input_config.get('mode', 'inc')
        self.last_processed_snapshot = self.load_checkpoint()

    def read(self, spark):

        full_table_name = f"{self.catalog_name}.{self.database}.{self.table}"
        history_df = spark.sql(f"SELECT * FROM {full_table_name}.history")
        history_df.createOrReplaceTempView("table_history")

        latest_snapshot = \
            spark.sql("SELECT snapshot_id FROM table_history ORDER BY made_current_at DESC LIMIT 1").collect()[0][0]

        if self.mode == 'full' or self.last_processed_snapshot is None:
            print("Processing all data.")
            df = spark.read.format("iceberg").table(full_table_name)
        elif latest_snapshot == self.last_processed_snapshot:
            print("No new data to process.")
            return None
        else:
            print(f"Processing data from snapshot {self.last_processed_snapshot} to {latest_snapshot}")
            df = spark.read.format("iceberg") \
                .option("start-snapshot-id", self.last_processed_snapshot) \
                .option("end-snapshot-id", latest_snapshot) \
                .table(full_table_name)

        self.last_processed_snapshot = latest_snapshot
        return self.apply(spark, df)

    def load_checkpoint(self):
        checkpoint_data = self.load()
        return checkpoint_data.get('last_processed_snapshot') if checkpoint_data else None

    def commit_checkpoint(self):
        self.save({'last_processed_snapshot': self.last_processed_snapshot})
        print(f"Checkpoint updated to snapshot: {self.last_processed_snapshot}")


class ParquetSource(FileSource, TransformQuery):
    def __init__(self, input_config):
        super().__init__(input_config)
        TransformQuery.__init__(self, query=input_config.get("transform_query", ""))

    def read(self, spark):
        files = self.get_files()

        if not files:
            print("No files to process")
            return None

        df = spark.read.options.parquet(files)
        return self.apply(spark, df)


# ==================================================================
# Schema Class
# ====================================================================
class AvroSchema(Schema):

    def read_schema(self, path, spark):
        parsed_url = urlparse(path)
        if parsed_url.scheme in ['s3', 's3a']:
            schema_json = self._read_s3_file_schema(parsed_url)
        else:
            with open(path, 'r') as f:
                schema_json = f.read()

        avro_schema = json.loads(schema_json)
        spark_schema = self._avro_to_spark_schema(avro_schema)
        return spark.createDataFrame([], schema=spark_schema)

    def _read_s3_file_schema(self, parsed_url):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
        return response['Body'].read().decode('utf-8')


    def _avro_to_spark_schema(self, avro_schema):
        def convert_type(avro_type):
            type_mapping = {
                'string': StringType(),
                'int': IntegerType(),
                'long': LongType(),
                'float': FloatType(),
                'double': DoubleType(),
                'boolean': BooleanType(),
                'timestamp-micros': TimestampType(),
                'date': DateType()
            }
            if isinstance(avro_type, dict):
                if avro_type.get('logicalType') == 'timestamp-micros':
                    return TimestampType()
                elif avro_type.get('logicalType') == 'date':
                    return DateType()
            return type_mapping.get(avro_type, StringType())

        fields = []
        for field in avro_schema['fields']:
            field_type = field['type']
            if isinstance(field_type, dict):
                spark_type = convert_type(field_type)
                nullable = True
            elif isinstance(field_type, list):
                non_null_type = next(t for t in field_type if t != 'null')
                spark_type = convert_type(non_null_type)
                nullable = 'null' in field_type
            else:
                spark_type = convert_type(field_type)
                nullable = False
            fields.append(StructField(field['name'], spark_type, nullable))
        return StructType(fields)


# ==================================================================
# Merge Query Class
# ====================================================================
class MergeQuery:
    def __init__(self, merge_query_path):
        self.merge_query_path = merge_query_path
        self.parsed_url = urlparse(merge_query_path)

    def read_merge_query(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._read_s3_file()
        else:
            return self._read_local_file()

    def _read_s3_file(self):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'))

        return response['Body'].read().decode('utf-8')


    def _read_local_file(self):
        with open(self.merge_query_path, 'r') as f:
            return f.read()

    def execute_merge(self, spark, df):

        merge_query = self.read_merge_query()
        df.createOrReplaceTempView("source_table")
        df = spark.sql(merge_query)
        print("MERGED DF")
        df.show()
        spark.catalog.dropTempView("source_table")


# ==================================================================
# Targets
# ====================================================================
class TargetIcebergTableBuckets(Target, AvroSchema, MergeQuery):
    def __init__(self, output_config):

        AvroSchema.__init__(self)
        MergeQuery.__init__(self, merge_query_path=output_config.get("merge_query"))
        self.output_config = output_config
        self.table_name = output_config['table_name']
        self.mode = output_config.get('mode', 'append')
        self.schema = output_config['schema']
        self.table_type = output_config.get('table_type', 'COW')
        self.compression = output_config.get('compression', 'zstd')
        self.partition = output_config.get('partition', '')

    def write(self, df, spark):

        full_table_name = f"{self.output_config['catalog_name']}.{self.output_config['database']}.{self.output_config['table_name']}"
        table_exists = spark.catalog.tableExists(full_table_name)


        if not table_exists:
            print(f"Table {full_table_name} does not exist. Creating new table with provided schema.")
            empty_df = self.read_schema(
                path=self.output_config.get("schema"),
                spark=spark
            )
            print("empty_df", empty_df.printSchema())
            table_properties = {
                'write.delete.mode': 'merge-on-read',
                'write.update.mode': 'merge-on-read',
                'write.merge.mode': 'merge-on-read'
            } if self.table_type.upper() == 'MOR' else {
                'write.delete.mode': 'copy-on-write',
                'write.update.mode': 'copy-on-write',
                'write.merge.mode': 'copy-on-write'
            }

            writer = empty_df.writeTo(full_table_name).using("iceberg").tableProperty("format-version", "2")

            for key, value in table_properties.items():
                writer = writer.tableProperty(key, value)

            if self.partition:
                writer = writer.partitionedBy(self.partition)

            writer.create()
            print(f"Created Iceberg Tables {full_table_name}")

        if self.mode == 'append':
            df.write.format("iceberg").mode("append").saveAsTable(full_table_name)
        elif self.mode == 'overwrite':
            df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
        elif self.mode == 'merge':
            print("in merge command ")
            self.execute_merge(
                spark, df
            )

        else:
            raise ValueError(f"Unsupported write mode: {self.mode}")


# ==================================================================
# Factory Methods
# ====================================================================

def create_source(input_config):
    source_type = input_config['type'].lower()
    print("source_type***", source_type)
    if source_type == 'csv':
        return CsvSource(input_config)
    elif source_type == 'parquet':
        return ParquetSource(input_config)
    elif source_type == 'iceberg':
        return IcebergSource(input_config)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


# ==================================================================
# Build Spark Session
# ====================================================================
def create_spark_session(spark_config):
    builder = SparkSession.builder
    default = {
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    for key, value in default.items():
        builder = builder.config(key, value)

    for key, value in spark_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def load_config(config_path):
    parsed_url = urlparse(config_path)

    if parsed_url.scheme in ['s3', 's3a']:
        # S3 path
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
        config_content = response['Body'].read().decode('utf-8')
    else:
        # Local path
        with open(config_path, 'r') as f:
            config_content = f.read()

    return json.loads(config_content)


def run(input_config, spark_config, output_config):
    spark = create_spark_session(spark_config=spark_config)

    try:
        source = create_source(input_config)
        print("source ready")
        df = source.read(spark)
        if df is not None:
            df.show(truncate=True)
            if output_config != "":
                target = TargetIcebergTableBuckets(
                    output_config=output_config
                )
                target.write(df, spark)

            if input_config.get("commit_checkpoint", True) == True: source.commit_checkpoint()
        else:
            print("No data to process")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_config_file>")
        sys.exit(1)

    config_path = sys.argv[1]
    print("config_path", config_path)
    config = load_config(config_path)
    print(json.dumps(config, indent=3))

    run(input_config=config.get("input_config"),
        spark_config=config.get("spark"),
        output_config=config.get("output_config"))
