from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pythontest").getOrCreate()

text = "Hello Airflow Hello Docker Hello Anoop"

words = spark.sparkContext.parallelize(text.split(" "))

wordcount = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:a + b)

for wc in wordcount.collect():
    print(wc[0], wc[1])

spark.stop()