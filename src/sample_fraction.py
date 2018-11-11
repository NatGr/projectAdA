"""
sample a fraction of the data from the parquet files so that we can download it locally to perform tests
"""
import os
from pyspark.sql import *

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

EXTRACT_PERCENTAGE = 1
PARQUET_BASE_FOLDER = "hdfs:///user/greffe"
PARQUET_FOLDER_NAME_EVENT = os.path.join(PARQUET_BASE_FOLDER, "Event.parquet")
PARQUET_FOLDER_NAME_MENTIONS = os.path.join(PARQUET_BASE_FOLDER, "Mentions.parquet")
PARQUET_FOLDER_NAME_EVENT_PERCENTAGE = "Event_percentage{}.parquet".format(EXTRACT_PERCENTAGE)
PARQUET_FOLDER_NAME_MENTIONS_PERCENTAGE = "Mentions_percentage{}.parquet".format(EXTRACT_PERCENTAGE)

events = spark.read.parquet(PARQUET_FOLDER_NAME_EVENT)
mentions = spark.read.parquet(PARQUET_FOLDER_NAME_MENTIONS)

events_percentage = events.sample(fraction=float(EXTRACT_PERCENTAGE) / 100., withReplacement=False)
events_percentage.registerTempTable('events')
mentions.registerTempTable('mentions')

mentions_percentage_query = """
SELECT *
FROM mentions
WHERE mentions.EventId IN (SELECT Id from events)
"""  # select the mentions to the evenement in events_1_100
mentions_percentage = spark.sql(mentions_percentage_query)

# save as parquet
events_percentage.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_EVENT_PERCENTAGE)
mentions_percentage.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_MENTIONS_PERCENTAGE)