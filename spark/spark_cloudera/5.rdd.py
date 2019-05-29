from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("rdd").master("local").getOrCreate()
sc = spark.sparkContext


"""
Resilient Distributed Dataset (RDD)
─ Resilient: If data in memory is lost, it can be recreated
─ Distributed: Processed across the cluster
─ Dataset: Initial data can come from a source such as a file, or it can be
created programmatically
"""
"""
RDDs are unstructured
─ No schema defining columns and rows
─ Not table-like; cannot be queried using SQL-like transformations such as
where and select
─ RDD transformations use lambda functions
─ RDDs are often used to convert unstructured or semi-structured data into
structured form
"""
"""
SparkContext.textFile reads newline-terminated text files
─ Accepts a single file, a directory of files, a wildcard list of files, or a commaseparated
list of files
─ Examples
─ textFile("myfile.txt")
─ textFile("mydata/")
─ textFile("mydata/*.log")
─ textFile("myfile1.txt,myfile2.txt")
"""
row = Row("value")
myRDD = sc.textFile("purplecow.txt")
myRDD.map(row).toDF().show()

for line in myRDD.map(row).toDF().take(4):
    print(line)

myRDD.saveAsTextFile("mydata/")

userRDD = sc.wholeTextFiles("userfiles/")
userRDD.map(row).toDF().show()
print(userRDD.map(row).toDF().take(1))

"""
Some common actions
─ count returns the number of elements
─ first returns the first element
─ take(n) returns an array (Scala) or list (Python) of the first n elements
─ collect returns an array (Scala) or list (Python) of all elements
─ saveAsTextFile(dir) saves to text files
"""
