# from pyspark import SparkContext
from typing import List, Any, Union

from pyspark.sql import SparkSession

# sc = SparkContext(master="local", appName="Spark demo")
spark = SparkSession.builder.master("local").appName("user").getOrCreate()

usersDF = spark.read.json("user.json")
usersDF.show(3)
usersDF.printSchema()
print(usersDF.count())
users2 = usersDF.take(2)
print(users2)

# Transformations
nameAgeDF = usersDF.select("name", "age")
nameAgeDF.show()

over20DF = usersDF.where("age>20")
over20DF.show()

nameAgeOver20DF = usersDF.select("name", "age").where("age>20")
nameAgeOver20DF.show()

# chaining
usersDF.select("name", "age").where("age>20").show()


