"""
get all the data related to the inner_view of Afganistan and Mexico as well as the outer_view of Ivory Coast
to see what the distributions look like
"""

import os
from pyspark.sql import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

DATA_DIR = "hdfs:///user/greffe"
OUT_DIR = "hdfs:///user/greffe"
PARQUET_FOLDER_NAME_EVENT = os.path.join(DATA_DIR, "Event.parquet")
PARQUET_FOLDER_NAME_MENTIONS = os.path.join(DATA_DIR, "Mentions.parquet")
AFG_INNER = os.path.join(OUT_DIR, "afg_inner.csv")
MEXICO_INNER = os.path.join(OUT_DIR, "mexico_inner.csv")
IVORY_OUTER = os.path.join(OUT_DIR, "ivory_outer.csv")


events = spark.read.parquet(PARQUET_FOLDER_NAME_EVENT)
mentions = spark.read.parquet(PARQUET_FOLDER_NAME_MENTIONS)
events.registerTempTable('events')
mentions.registerTempTable('mentions')

join_query = """
SELECT events.Id, events.Date AS EventDate, events.Country AS ActorCountry, events.Type AS ActorType, 
mentions.Date AS MentionDate, mentions.SourceName AS SourceName, mentions.Country AS MentionCountry, 
mentions.Confidence AS Confidence, mentions.Tone AS Tone
FROM mentions INNER JOIN events ON events.Id = mentions.EventId
"""  # join the two tables and group it by the actor and the mention country

joined_table = spark.sql(join_query)
joined_table.registerTempTable('joined_table')


query="""
SELECT SourceName AS source_name, Tone AS tone
FROM joined_table
WHERE ActorCountry = '{}' AND MentionCountry {} '{}'
"""

for country, symbol, folder in zip(["Afghanistan", "Mexico", "Ivory Coast"], ["=", "=", "<>"], [AFG_INNER, MEXICO_INNER, IVORY_OUTER]):
	result = spark.sql(query.format(country, symbol, country))
	result.write.format('com.databricks.spark.csv').save(folder)