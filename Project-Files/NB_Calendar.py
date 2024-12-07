#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd


# In[7]:


import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp")
# Spark will automatically use the master specified in spark-defaults.conf
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark


# In[5]:


trgt_path_processed = "/mnt/Calendar/Calendar_Parquet/"
trgt_path_csv = "/mnt/Calendar/Calendar_Processed/"


# In[6]:


# Create a DataFrame with date range
start_date = "2000-01-01"
end_date = "2050-12-31"


# In[7]:


# Create a DataFrame with a single row containing the start and end date
date_range_df = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])

# Generate date sequence
date_sequence_df = date_range_df.select(
    sequence(
        to_date(date_range_df.start_date).alias("start_date"),
        to_date(date_range_df.end_date).alias("end_date")
    ).alias("date")
)


# In[8]:


# Explode the sequence into separate rows
df_date = date_sequence_df.selectExpr("explode(date) as date")


# In[9]:


df_output = df_date.withColumn("DateSK", regexp_replace("date", "-", "")).withColumn("Year", year("date"))\
    .withColumn("Month",date_format("date","MMMM")).withColumn("Quarter",concat(year("date"), lit(" Q"), quarter("date")))


# In[10]:


print(df_output.count())


# In[11]:


df_output.createOrReplaceTempView("vw_source")


# In[12]:


column_name = df_output.columns
set_clause = ", ".join([f"target.{i} = source.{i}" for i in column_name])
query = f"""MERGE INTO delta.`{trgt_path_processed}` AS target USING vw_source AS source ON target.DateSK = source.DateSK WHEN MATCHED THEN UPDATE SET {set_clause}"""
print(query)


# In[13]:


if DeltaTable.isDeltaTable(spark, trgt_path_processed):
    spark.sql(query)
else :
    spark.sql(f"""CREATE TABLE delta.`{trgt_path_processed}` USING DELTA AS SELECT * FROM vw_source""")


# In[14]:


df_output.write.format("delta").mode("overwrite").save(trgt_path_processed)


# In[15]:


# Save the DataFrame to a CSV file
spark.read.format("delta").load(trgt_path_processed).coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(trgt_path_csv)


# In[16]:


spark.read.format("delta").load(trgt_path_processed).count()


# In[ ]:




