from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SERVER_HOST = "http://localhost:9000"


spark = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.3.1,"
        "software.amazon.awssdk:bundle:2.16.60,"
        "software.amazon.awssdk:url-connection-client:2.16.60,"
        "org.postgresql:postgresql:42.6.0,"
        "org.apache.hadoop:hadoop-aws:3.1.1,"
        "com.amazonaws:aws-java-sdk:1.11.271,"
        # "org.apache.iceberg:iceberg-aws:1.3.0"
    )
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_SERVER_HOST)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    # config catalog
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.default', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.default.type', 'hive')
    .config('spark.sql.catalog.default.uri', 'thrift://localhost:9083')
    .config('spark.sql.catalog.default.warehouse', 's3a://datalake/')
    .config('spark.sql.catalog.default.s3.endpoint', MINIO_SERVER_HOST)
    .getOrCreate()
)

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# df.writeTo("default.table1").using("iceberg").create()
df.write.format('iceberg').insertInto("table1")
# df.write.format("iceberg").mode("overwrite").sa("iceberg.default.table1")
# df.createOrReplaceTempView("temp")
spark.sql("select * from iceberg_table").show()


