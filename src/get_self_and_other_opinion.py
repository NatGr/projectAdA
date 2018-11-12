"""
return the opinion of a country about itself as well as the opinion of other countries about the later 
the output pandas dataframes will contain
		-inner_view:
			-Country
			-avg_tone
			-stddev_tone
			-count
			-avg_conf
			-avg_weighted_tone
			-std_weighted_tone
		-outer_view:
			-ActorCountry
			-MentionsCountry
			-avg_tone
			-stddev_tone
			-count
			-avg_conf
			-avg_weighted_tone
			-std_weighted_tone
"""

import os
from pyspark.sql import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

DATA_DIR = "hdfs:///user/greffe"
OUT_DIR = "hdfs:///user/greffe"
PARQUET_FOLDER_NAME_EVENT = os.path.join(DATA_DIR, "Event.parquet")
PARQUET_FOLDER_NAME_MENTIONS = os.path.join(DATA_DIR, "Mentions.parquet")
OUTPUT_INNER_VIEW = os.path.join(OUT_DIR, "inner_view.csv")
OUTPUT_OUTER_VIEW = os.path.join(OUT_DIR, "outer_view.csv")

events = spark.read.parquet(PARQUET_FOLDER_NAME_EVENT)
mentions = spark.read.parquet(PARQUET_FOLDER_NAME_MENTIONS)
events.registerTempTable('events')
mentions.registerTempTable('mentions')

join_query="""
SELECT events.Id, events.Date AS EventDate, events.Country AS ActorCountry, events.Type AS ActorType,\
 mentions.Date AS MentionDate, mentions.SourceName AS SourceName, mentions.Country AS MentionCountry,\
  mentions.Confidence AS Confidence, mentions.Tone AS Tone
FROM mentions INNER JOIN events ON events.Id = mentions.EventId
"""  # join the two tables and group it by the actor and the mention country

joined_table = spark.sql(join_query)
joined_table.registerTempTable('joined_table')

# view of a country about itself
inner_view_query="""
SELECT ActorCountry AS Country, avg(Tone) AS avg_tone, stddev(Tone) AS std_tone, count(Tone) AS count, \
avg(Confidence) AS avg_conf, avg(Tone*Confidence) AS avg_weighted_tone, stddev(Tone*Confidence) AS std_weighted_tone
FROM joined_table
GROUP BY ActorCountry, MentionCountry
HAVING ActorCountry = MentionCountry
"""  # statistical info about Tone

inner_view = spark.sql(inner_view_query)
inner_view = inner_view.withColumn("avg_weighted_tone", inner_view.avg_weighted_tone/inner_view.avg_conf)\
.withColumn("std_weighted_tone", inner_view.std_weighted_tone/inner_view.avg_conf)  # correct the weighted tone 
# so that it's on the "same" scale as the tone
inner_view.write.format('com.databricks.spark.csv').save(OUTPUT_INNER_VIEW)

# view of a country from other countries
outer_view_query="""
SELECT ActorCountry, MentionCountry, avg(Tone) AS avg_tone, stddev(Tone) AS std_tone, count(Tone) AS count, \
avg(Confidence) AS avg_conf, avg(Tone*Confidence) AS avg_weighted_tone, stddev(Tone*Confidence) AS std_weighted_tone
FROM joined_table
GROUP BY ActorCountry, MentionCountry
HAVING ActorCountry <> MentionCountry
"""  # statistical info about Country

outer_view = spark.sql(outer_view_query)
outer_view = outer_view.withColumn("avg_weighted_tone", outer_view.avg_weighted_tone/outer_view.avg_conf)\
.withColumn("std_weighted_tone", outer_view.std_weighted_tone/outer_view.avg_conf)  # correct the weighted tone 
# so that it's on the "same" scale as the tone
outer_view.write.format('com.databricks.spark.csv').save(OUTPUT_OUTER_VIEW)
