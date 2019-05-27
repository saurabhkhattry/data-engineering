from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("joins").master("local").getOrCreate()

# Joining Dataframes
"""
▪ DataFrames support several types of joins
─ inner (default)
─ outer
─ left_outer
─ right_outer
─ leftsemi
─ crossJoin
"""

peopleDF = spark.read\
    .option("Header", "True")\
    .csv("people.csv")

pcodeDF = spark.read\
    .option("Header", "True")\
    .csv("pcode.csv")

peopleDF.join(pcodeDF, "pcode").show()

