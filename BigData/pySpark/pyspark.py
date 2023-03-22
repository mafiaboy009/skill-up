from pyspark.sql import *
import pandas as pd

spark = SparkSession.builder.appName("pyspark-test-app").getOrCreate()

df = spark.read.csv("./data/userdata.csv", header="true", inferSchema="true")
df.createOrReplaceTempView("users")

sql = """ select gender, AVG(age) as average_age from users group by gender"""
query = spark.sql(sql)
query.toPandas()