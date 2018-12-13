"""
return the opinion of a media about a country the output dataframe will contain: 
(actor_country and mention_country could be the same)
	output dataframe Columns:
		-actor_country
		-source_country
		-source_name
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
OUTPUT_TYPE_VIEW = os.path.join(OUT_DIR, "media_to_country_view.csv")

joined_table = perform_join(spark)

# view of a country about itself
media_query="""
SELECT ActorCountry AS actor_country, 
MentionCountry AS source_country,
SourceName AS source_name,
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
GROUP BY ActorCountry, MentionCountry, SourceName
"""  # statistical info about Tone, we group by MentionCountry so that we can output it

media_view = spark.sql(media_query)

media_view = media_view.withColumn("avg_weighted_tone", media_view.avg_weighted_tone/media_view.avg_conf)\
.withColumn("std_weighted_tone", media_view.std_weighted_tone/media_view.avg_conf)  # correct the weighted tone 
# so that it's on the "same" scale as the tone

# sparks stddev function returns NaN when there is one data to compute the std from, we replace this by a 0 in that case
media_view = media_view.withColumn("std_tone", when(isnan(col("std_tone"))
 & (col("count_mentions") == 1), 0).otherwise(media_view.std_tone))
media_view = media_view.withColumn("std_weighted_tone", when(isnan(col("std_weighted_tone"))
 & (col("count_mentions") == 1), 0).otherwise(media_view.std_weighted_tone))

media_view.write.format('com.databricks.spark.csv').save(OUTPUT_TYPE_VIEW)