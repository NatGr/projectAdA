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
MAPPING_DIR = "../data"  # TODO: change
URL_MAPPING_FILE = os.path.join(MAPPING_DIR, "DOMAINSBYCOUNTRY-ALLLANGUAGES.TXT")
COUNTRY_CODES_MAPPING_FILE = os.path.join(MAPPING_DIR, "mapping_country_codes.csv")
PARQUET_FOLDER_NAME_EVENT = "Event.parquet"
PARQUET_FOLDER_NAME_MENTIONS = "Mentions.parquet"

# get list of event and mentions files
event_regex = re.compile(r'.*\.export\.CSV')  
event_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if event_regex.match(f)]
mentions_regex = re.compile(r'.*\.mentions\.CSV')
mentions_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if mentions_regex.match(f)]

# read the export.csv files
print("Reading the files")
event = spark.read.csv(event_files, sep="\t")
event = event.select(event._c0.cast("bigint").alias("Id"),
	to_date(event._c1, "yyyymmdd").alias("Date"),
	event._c7.alias("Country"),
	event._c12.alias("Type"),
	event._c34.cast("double").alias("AvgTone"))  # selecting columns of interest and casting them
event = event.dropna(how='any', subset=("Id", "Date", "Country", "AvgTone"))  # we accept to not have a Type but the other fields are necessary
event.registerTempTable('event')
print(f"event treated, {event.count()} rows left in event")

# transform countries code into countries names
# data was taken from https://datahub.io/core/country-codes#data on 7th of november 2018 and then adapted to match the 
# countries names used in the GDELT dataset (some countries did not have exactly the same name as mapping_url)
mapping_country_codes = spark.read.csv(COUNTRY_CODES_MAPPING_FILE, header=True)
mapping_country_codes = mapping_country_codes.select(mapping_country_codes.official_name_en, 
	mapping_country_codes["ISO3166-1-Alpha-3"].alias("ISO"))
mapping_country_codes.registerTempTable('mappingcc')
replace_country_code_by_name_query="""
SELECT Id, Date, official_name_en AS Country, Type, AvgTone
FROM event INNER JOIN mappingcc ON event.Country = mappingcc.ISO
"""  # replace the country codes by country names
event = spark.sql(replace_country_code_by_name_query)
print(f"countries codes replaced by countries names, {event.count()} rows left in event")
print("it is normal to lose them as some actors are not labelled by countries but by continent (AFR, ASA, EUR, ...))")

# read the mentions.csv files
mentions = spark.read.csv(mentions_files, sep="\t")
mentions = mentions.select(mentions._c0.cast("bigint").alias("EventId"),
	to_date(mentions._c2.substr(0,8), "yyyymmdd").alias("Date"),  # we do not take the chars correspodning to hours mins and seconds
	mentions._c4.alias("SourceName"),
	mentions._c11.cast("int").alias("Confidence"),
	mentions._c13.cast("double").alias("Tone"))
mentions = mentions.dropna(how='any')
mentions.registerTempTable('mentions')
print(f"mentions treated, {mentions.count()} rows left in mentions")

# remove mentions that do not concern the events we kept
remove_mentions_query="""
SELECT *
FROM mentions
WHERE mentions.EventId NOT IN (SELECT Id from event)
"""
mentions = spark.sql(remove_mentions_query)
mentions.registerTempTable('mentions')  # need to redo this otherwise it will work with the previous table
print(f"removing mentions about unknown events, {mentions.count()} rows left in mentions")

# read the mapping file and replace the url by their mappings in mentions
# data was taken from https://blog.gdeltproject.org/multilingual-source-country-crossreferencing-dataset/ on 30th october 2018
# mapping_url.Country was checked to be a subset of mapping_country_codes.official_name_en, there is only one entry
# that soesn't corresponds (pabsec.org	OS	Oceans), we were unable to find what that country is and we are thus ignoring that entry in the later
mapping_url = spark.read.csv(URL_MAPPING_FILE, sep="\t", header=True)  # use the first line as column labels
mapping_url = mapping_url.select(mapping_url.SourceCommonName.alias("url"), mapping_url._c2.alias("Country"))
mapping_url.registerTempTable('mappingurl')
replace_countries_query="""
SELECT EventId, Date, Country, Confidence, Tone
FROM mentions INNER JOIN mappingurl ON mentions.SourceName = mappingurl.url
"""  # transforms urls into source countries
mentions = spark.sql(replace_countries_query)
print(f"mentions url_mapping, {mentions.count()} rows left in mentions")
print("it is normal that we lose some of them since the GDELT file doesn't contains all of the urls, see doc)")

# save as parquet
event.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_EVENT)
mentions.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_MENTIONS)
print("data written in parquet files")