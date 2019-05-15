from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").appName("dataframe_basics").getOrCreate()

"""
Handling parquet files:
Use head to display the first few records
parquet-tools head datafile_name.parquet
Use schema to view the schema
parquet-tools schema datafile_name.parquet
"""

"""
Use DataFrameReader settings to specify how to load data from the data
source
─ format indicates the data source type, such as csv, json, or parquet
(the default is parquet)
─ option specifies a key/value setting for the underlying data source
─ schema specifies a schema to use instead of inferring one from the data
source
- load loads data from a file or files
─ table loads data from a Hive table
"""
# Read a CSV text file
myDF = spark.read. \
    format("csv"). \
    option("header", "true"). \
    option("inferSchema", "true"). \
    load("diamonds.csv")

myDF.show(5)

myDF.printSchema()

# Read a table defined in the Hive metastore
# myDF = spark.read.table("table_name")

"""
You must specify a location when reading from a file data source
─ The location can be a single file, a list of files, a directory, or a wildcard
─ Examples
─ spark.read.json("myfile.json")
─ spark.read.json("mydata/")
─ spark.read.json("mydata/*.json")
─ spark.read.json("myfile1.json","myfile2.json")
"""

"""
DataFrameWriter methods
─ format specifies a data source type
─ mode determines the behavior if the directory or table already exists
─ error, overwrite, append, or ignore (default is error)
─ partitionBy stores data in partitioned directories in the form
column=value (as with Hive/Impala partitioning)
─ option specifies properties for the target data source
─ save saves the data as files in the specified directory
─ Or use json, csv, parquet, and so on
─ saveAsTable saves the data to a Hive metastore table
─ Uses default table location (/user/hive/warehouse)
─ Set path option to override location
"""
local_path = "C:\\Users\\saurabh\\git\\data-engineering\\spark\\spark_cloudera\\diamond_parquet\\"
myDF.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("cut") \
    .save(local_path)


peopleDF = spark.read \
    .format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .load("people.csv")

peopleDF.printSchema()

peopleSchema = StructType([
    StructField("pcode", StringType()),
    StructField("lastName", StringType()),
    StructField("firstName", StringType()),
    StructField("age", IntegerType())
])

spark.read.option("header", "True") \
    .schema(peopleSchema) \
    .csv("people.csv") \
    .printSchema()
