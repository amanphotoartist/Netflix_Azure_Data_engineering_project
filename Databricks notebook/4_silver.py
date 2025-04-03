# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver data transformation

# COMMAND ----------

df=spark.read.format('delta').option('header',True).option('inferSchema',True).load('abfss://bronze@netflixprojectdlaman.dfs.core.windows.net/netflix_titles')

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.fillna({'duration_minutes':0,'duration_seasons':1})

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('duration_minutes',col('duration_minutes').cast(IntegerType())).withColumn('duration_seasons',col('duration_seasons').cast(IntegerType()))

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('Shorttitle',split(col('title'),':')[0])
df.display()

# COMMAND ----------

df=df.withColumn('rating',split(col('rating'),'-')[0])
df.display()

# COMMAND ----------

df=df.withColumn('type_flag',when(col('type')=='Movie',1).otherwise(0))
df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df=df.withColumn('duration_ranking',dense_rank().over(Window.orderBy(col('duration_minutes').desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

df_vis=df.groupBy('type').agg(count('*').alias('count'))
df.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('path', 'abfss://silver@netflixprojectdlaman.dfs.core.windows.net/netflix_titles').option('mergeSchema', 'true').save()

# COMMAND ----------

