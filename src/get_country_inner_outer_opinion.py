"""
return the opinion of a country from inside of it and from the outer world
	both output dataframes (inner view and outer view) have the same columns:
		-country
		-avg_tone
		-stddev_tone
		-count_mentions: number of distinct mentions
		-count_events: number of distinct events
		-avg_conf
		-avg_weighted_tone
		-std_weighted_tone
		-first_quartile_tone
		-median_tone
		-third_quartile_tone
"""

import os
from pyspark.sql import *
from pyspark.sql.functions import isnan, when, col

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

DATA_DIR = "hdfs:///user/greffe"
OUT_DIR = "hdfs:///user/greffe"
PARQUET_FOLDER_NAME_EVENT = os.path.join(DATA_DIR, "Event.parquet")
PARQUET_FOLDER_NAME_MENTIONS = os.path.join(DATA_DIR, "Mentions.parquet")
OUTPUT_INNER_VIEW = os.path.join(OUT_DIR, "country_inner_view.csv")
OUTPUT_OUTER_VIEW = os.path.join(OUT_DIR, "country_outer_view.csv")

events = spark.read.parquet(PARQUET_FOLDER_NAME_EVENT)
mentions = spark.read.parquet(PARQUET_FOLDER_NAME_MENTIONS)
events.registerTempTable('events')
mentions.registerTempTable('mentions')

join_query="""
SELECT events.Id, events.Date AS EventDate, events.Country AS ActorCountry, events.Type AS ActorType, 
mentions.Date AS MentionDate, mentions.SourceName AS SourceName, mentions.Country AS MentionCountry, 
mentions.Confidence AS Confidence, mentions.Tone AS Tone
FROM mentions INNER JOIN events ON events.Id = mentions.EventId
"""  # join the two tables and group it by the actor and the mention country

joined_table = spark.sql(join_query)
joined_table.registerTempTable('joined_table')

# view of a country about itself
queries = """
SELECT ActorCountry AS country, 
avg(Tone) AS avg_tone, 
stddev(Tone) AS std_tone, 
count(Tone) AS count_mentions, 
count(DISTINCT Id) AS count_events, 
avg(Confidence) AS avg_conf, 
avg(Tone*Confidence) AS avg_weighted_tone, 
stddev(Tone*Confidence) AS std_weighted_tone, 
percentile_approx(Tone, 0.25) AS first_quartile_tone, 
percentile_approx(Tone, 0.5) AS median_tone, 
percentile_approx(Tone, 0.75) AS third_quartile_tone
FROM joined_table
WHERE ActorCountry {} MentionCountry
GROUP BY ActorCountry
"""  # statistical info about Tone, formatted with an equal for the inner view of the country or a not equal for the outer view

# we execute the body of the loop with an equal sign for inner view and a sql not equal for outer_view
for out_folder, sign in [(OUTPUT_INNER_VIEW, "="), (OUTPUT_OUTER_VIEW, "<>")]:
	view = spark.sql(queries.format(sign))

	view = view.withColumn("avg_weighted_tone", view.avg_weighted_tone/view.avg_conf)\
	.withColumn("std_weighted_tone", view.std_weighted_tone/view.avg_conf)  # correct the weighted tone 
	# so that it's on the "same" scale as the tone

	# sparks stddev function returns NaN when there is one data to compute the std from, we replace this by a 0 in that case
	view = view.withColumn("std_tone", when(isnan(col("std_tone"))
	 & (col("count_mentions") == 1), 0).otherwise(view.std_tone))
	view = view.withColumn("std_weighted_tone", when(isnan(col("std_weighted_tone"))
	 & (col("count_mentions") == 1), 0).otherwise(view.std_weighted_tone))

	view.write.format('com.databricks.spark.csv').save(out_folder)