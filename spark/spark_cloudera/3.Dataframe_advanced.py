from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("dataframe_advanced").getOrCreate()

"""
Column operations include
─ Arithmetic operators such as +, -, %, /, and *
─ Comparative and logical operators such as >, <, && and ||
─ The equality comparator is === in Scala, and == in Python
─ String functions such as contains, like, and substr
─ Data testing functions such as isNull, isNotNull, and NaN (not a
number)
─ Sorting functions such as asc and desc
─ Work only when used in sort/orderBy
"""
peopleDF = spark.read\
    .option("header", "True")\
    .csv("people.csv")

peopleDF.where(peopleDF.firstName.startswith("A")).show()

peopleDF.select("lastName", (peopleDF.age*10).alias("age_10")).show()

# Aggregation query

"""
▪ groupBy takes one or more column names or references returns a GroupedData object
▪ Returned objects provide aggregation functions, including
─ count
─ max and min
─ mean (and its alias avg)
─ sum
─ pivot
─ agg (aggregates using additional aggregation functions)
"""

peopleDF.groupBy("pcode").count().show()



