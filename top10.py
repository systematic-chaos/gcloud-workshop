from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext("local")
sq = SQLContext(sc)

df_categories = sq.read.json('gs://gcloudworkshop2021/categories.json')
df_categories.printSchema()

df_resources = sq.read.json('gs://gcloudworkshop2021/resources.json')
df_resources.printSchema()

df_events = sq.read.json('gs://gcloudworkshop2021/events.json')
df_resources.printSchema()

# Using API
df_categories.join(df_resources, df_categories.categoryId == df_resources.categoryId).show()
df_resources.select('categoryId').show()
