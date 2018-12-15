"""
return the number of times the news sources belonging to each size cluster mention each news type
"""

import os
from pyspark.sql import *
from pyspark.sql.functions import isnan, when, col


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc.addFile("hdfs:///user/greffe/perform_join.py")
from perform_join import perform_join  # must be done after spark context

OUT_DIR = "hdfs:///user/greffe"
OUTPUT_TYPE_REPARTITION = os.path.join(OUT_DIR, "type_repartition.csv")
MAPPING_DIR = "hdfs:///user/greffe/data"
CLUSTER_MAPPING_FILE = os.path.join(MAPPING_DIR, "source_cluster.csv")

# get the cluster for each source
source_cluster = spark.read.csv(CLUSTER_MAPPING_FILE, header=True)
source_cluster.registerTempTable('source_cluster')

joined_table = perform_join(spark)


join_cluster_query="""
SELECT SourceName, 
MentionCountry,
ActorType,
Tone,
cluster
FROM joined_table INNER JOIN source_cluster ON joined_table.SourceName = source_cluster.source_name and joined_table.MentionCountry = source_cluster.source_country
WHERE joined_table.MentionCountry <> joined_table.ActorCountry
"""


joined_cluster_table = spark.sql(join_cluster_query)
joined_cluster_table.registerTempTable('joined_cluster_table')


# getting the average type
type_query="""
SELECT SourceName AS source_name, 
MentionCountry AS source_country,
ActorType AS actor_type,
cluster,
avg(Tone) AS avg_tone, 
stddev(Tone) AS std_tone, 
count(Tone) AS count_mentions,
FROM joined_cluster_table
GROUP BY SourceName, MentionCountry, ActorType, cluster
"""

type_table = spark.sql(type_query)

# sparks stddev function returns NaN when there is one data to compute the std from, we replace this by a 0 in that case
type_table = type_table.withColumn("std_tone", when(isnan(col("std_tone"))
 & (col("count_mentions") == 1), 0).otherwise(type_table.std_tone))

type_table.write.format('com.databricks.spark.csv').save(OUTPUT_TYPE_REPARTITION)