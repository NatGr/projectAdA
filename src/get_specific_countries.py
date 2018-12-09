"""
get all the data related to the inner_view of Afganistan, Mexico and Algeria as well as the outer_view of Ivory Coast and Tuvalu
to see what the distributions look like
"""

import os
from pyspark.sql import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc.addFile("hdfs:///user/greffe/perform_join.py")
from perform_join import perform_join  # must be done after spark context

OUT_DIR = "hdfs:///user/greffe"
AFG_INNER = os.path.join(OUT_DIR, "afg_inner.csv")
MEXICO_INNER = os.path.join(OUT_DIR, "mexico_inner.csv")
IVORY_OUTER = os.path.join(OUT_DIR, "ivory_outer.csv")
ALGERIA_INNER = os.path.join(OUT_DIR, "algeria_inner.csv")
TUVALU_OUTER = os.path.join(OUT_DIR, "tuvalu_outer.csv")

joined_table = perform_join(spark)


query="""
SELECT SourceName AS source_name, Tone AS tone
FROM joined_table
WHERE ActorCountry = '{}' AND MentionCountry {} '{}'
"""

for country, symbol, folder in zip(["Afghanistan", "Mexico", "Ivory Coast", "Algeria", "Tuvalu"], ["=", "=", "<>", "=", "<>"], [AFG_INNER, MEXICO_INNER, IVORY_OUTER, ALGERIA_INNER, TUVALU_OUTER]):
	result = spark.sql(query.format(country, symbol, country))
	result.write.format('com.databricks.spark.csv').save(folder)