"""
return the opinion of a country about its actors_types as well as the opinion of other countries about the later 
the output dataframe will contain: (actor_country and mention_country could be the same)
	output dataframe Columns:
		-actor_country
		-mention_country
		-actor_type
		-avg_tone
		-std_tone
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

sc.addFile("hdfs:///user/greffe/perform_join.py")
from perform_join import perform_join  # must be done after spark context

OUT_DIR = "hdfs:///user/greffe"
OUTPUT_TYPE_VIEW = os.path.join(OUT_DIR, "country_to_country_type_view.csv")

joined_table = perform_join(spark)

# view of a country about itself
type_query="""
SELECT ActorCountry AS actor_country, 
MentionCountry AS mention_country,
ActorType AS actor_type,
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
WHERE ActorType IS NOT NULL
GROUP BY ActorCountry, MentionCountry, ActorType
"""  # statistical info about Tone

type_view = spark.sql(type_query)

type_view = type_view.withColumn("avg_weighted_tone", type_view.avg_weighted_tone/type_view.avg_conf)\
.withColumn("std_weighted_tone", type_view.std_weighted_tone/type_view.avg_conf)  # correct the weighted tone 
# so that it's on the "same" scale as the tone

# sparks stddev function returns NaN when there is one data to compute the std from, we replace this by a 0 in that case
type_view = type_view.withColumn("std_tone", when(isnan(col("std_tone"))
 & (col("count_mentions") == 1), 0).otherwise(type_view.std_tone))
type_view = type_view.withColumn("std_weighted_tone", when(isnan(col("std_weighted_tone"))
 & (col("count_mentions") == 1), 0).otherwise(type_view.std_weighted_tone))

type_view.write.format('com.databricks.spark.csv').save(OUTPUT_TYPE_VIEW)