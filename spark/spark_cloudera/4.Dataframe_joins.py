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

print(" inner(default) ".center(50, '#'))
peopleDF.join(pcodeDF, "pcode").show()

"""
Specify type of join as inner (default), outer, left_outer,
right_outer, or leftsemi
"""
print(" left_outer ".center(50, '#'))
peopleDF.join(pcodeDF, "pcode", "left_outer").show()
print(" right_outer ".center(50, '#'))
peopleDF.join(pcodeDF, "pcode", "right_outer").show()
print(" leftsemi ".center(50, '#'))
peopleDF.join(pcodeDF, "pcode", "leftsemi").show()
print(" outer ".center(50, '#'))
peopleDF.join(pcodeDF, "pcode", "outer").show()

zcodeDF = spark.read\
    .option("Header", "True")\
    .csv("zcode.csv")

print(" inner on col with different names ".center(50, '#'))
peopleDF.join(zcodeDF, peopleDF.pcode == zcodeDF.zip).show()
