# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from dotenv import load_dotenv
import os

load_dotenv()

# COMMAND ----------

# Extract
spark = SparkSession.builder.appName("ETLPipeline").getOrCreate()
df = spark.read.text("WordData.txt")
print(df.show())

# COMMAND ----------

# Transformation
df2 = df.withColumn("splittedData", f.split("value", " "))
df3 = df2.withColumn("words", f.explode("splittedData"))
wordsDF = df3.select("words")
wordsDF = wordsDF.groupBy("words").count()
print(wordsDF.show())

# # COMMAND ----------

# #Load

driver = "org.postgresql.Driver"
table = "marcus_schema_pyspark.WordCount"
user = os.getenv("USER")
password = os.getenv("PASSWORD")
url = os.getenv("URL") + f'?user={user}&password={password}'
properties = {user: user, password: password, driver: driver}

wordsDF.write.jdbc(url, table, properties=properties)




