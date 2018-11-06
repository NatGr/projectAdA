import pandas as pd
import numpy as np
import pickle
import os
import re

import findspark
findspark.init()

from pyspark.sql import *
from pyspark.sql.functions import unix_timestamp, udf, to_date
from pyspark.sql.types import ArrayType, StringType, IntegerType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext


DATA_DIR = '../temp'  # TODO: change
URL_MAPPING_DIR = "../data"  # TODO: change
URL_MAPPING_FILE = os.path.join(URL_MAPPING_DIR, "DOMAINSBYCOUNTRY-ALLLANGUAGES.TXT")

# get list of event and mentions files
event_regex = re.compile(r'.*\.export\.CSV')  
event_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if event_regex.match(f)]
mentions_regex = re.compile(r'.*\.mentions\.CSV')
mentions_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if mentions_regex.match(f)]

# read the files
print("Reading the files")
event = spark.read.csv(event_files, sep="\t")
event = event.select(event._c0.cast("bigint").alias("Id"),
	to_date(event._c1, "yyyymmdd").alias("Date"),
	event._c7.alias("Country"),
	event._c12.alias("Type"),
	event._c34.cast("double").alias("AvgTone"))  # selecting columns of interest and casting them
event = event.dropna(how='any', subset=("GlobalEventID", "Date", "Country", "AvgTone"))  # drop the values that do not have any 
print(f"event treated, {event.count()} rows left")

mentions = spark.read.csv(mentions_files, sep="\t")
mentions = mentions.select(mentions._c0.cast("bigint").alias("EventId"),
	to_date(mentions._c2.substr(0,8), "yyyymmdd").alias("Date"),  # we do not take the chars correspodning to hours mins and seconds
	mentions._c4.alias("SourceName"),
	mentions._c11.cast("int").alias("Confidence"),
	mentions._c13.cast("double").alias("Tone"))
mentions = mentions.dropna(how='any')
mentions.registerTempTable('mentions')
print(f"mentions treated {mentions.count()} rows left")

# transform countries code into countries names


# read the mapping file and replace the url by their mappings in mentions
mapping_url = spark.read.csv(URL_MAPPING_FILE, sep="\t", header=True)  # use the first line as column labels
mapping_url = mapping_url.select(mapping_url.SourceCommonName.alias("url"), mapping_url._c2.alias("Country"))
mapping_url.registerTempTable('mappingurl')
replace_countries_query="""
select EventId, Date, Country, Confidence, Tone
from mentions inner join mappingurl on mentions.SourceName = mappingurl.url
"""  # transforms urls into source countries
mentions = spark.sql(replace_countries_query)
print(f"mentions after url_mapping: {mentions.count()}")

# remove mentions that do not concern event we kept


# save as parquet