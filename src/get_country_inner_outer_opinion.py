"""
return the opinion of a country from inside of it and from the outer world
	both output dataframes (inner view and outer view) have the same columns:
		-country
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
OUTPUT_INNER_VIEW = os.path.join(OUT_DIR, "country_inner_view.csv")
OUTPUT_OUTER_VIEW = os.path.join(OUT_DIR, "country_outer_view.csv")

joined_table = perform_join(spark)

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