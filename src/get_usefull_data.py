"""
get the data we are interested in from the 112Go GDELT csv files on the cluster and
save it as parquet files

-- output dataframes:

	-event:
		-Id: the id of the event
		-Date: the date of the event
		-Country: the country of the actor that provoked the event
		-Type: the Type of the actor that provoked the event
		-AvgTone: the average tone about the event
		
	-mentions:
		-EventId: the id of the correspongind event
		-Date: the date of the mention
		-SourceName: the name of the source of the mention
		-Country: the country of the mention
		-Confidence: the confidence GDELT has in it's parsing of the information
		-Tone: the tone used by the mention about the event
"""
import os

from pyspark.sql import *
from pyspark.sql.functions import to_date
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


DATA_DIR = 'hdfs:///datasets/gdeltv2'
MAPPING_DIR = "hdfs:///user/greffe/data"
URL_MAPPING_FILE = os.path.join(MAPPING_DIR, "DOMAINSBYCOUNTRY-ALLLANGUAGES.TXT")
COUNTRY_CODES_MAPPING_FILE = os.path.join(MAPPING_DIR, "mapping_country_codes.csv")
PARQUET_FOLDER_NAME_EVENT = "Event.parquet"
PARQUET_FOLDER_NAME_MENTIONS = "Mentions.parquet"


# put at the head of the file so that the job stops immediately if it cannot read them
mapping_country_codes = spark.read.csv(COUNTRY_CODES_MAPPING_FILE, header=True)
mapping_url = spark.read.csv(URL_MAPPING_FILE, sep="\t", header=True)  # use the first line as column labels


# read the export.csv files
print("Reading the files")
event = spark.read.csv(os.path.join(DATA_DIR, "*.export.CSV"), sep="\t")
event = event.select(event._c0.cast("bigint").alias("Id"),
	to_date(event._c1, "yyyymmdd").alias("Date"),
	event._c7.alias("Country"),
	event._c12.alias("Type"),
	event._c34.cast("double").alias("AvgTone"))  # selecting columns of interest and casting them

event = event.dropna(how='any', subset=("Id", "Date", "Country", "AvgTone"))  # we accept to not have a Type but the other fields are necessary
event.registerTempTable('event')
print("event treated, {} rows left in event".format(event.count()))


# transform countries code into countries names
# data was taken from https://datahub.io/core/country-codes#data on 7th of november 2018 and then adapted to match the 
# countries names used in the GDELT dataset (some countries did not have exactly the same name as mapping_url)
mapping_country_codes = mapping_country_codes.select(mapping_country_codes.official_name_en, 
	mapping_country_codes["ISO3166-1-Alpha-3"].alias("ISO"))
mapping_country_codes.registerTempTable('mappingcc')

replace_country_code_by_name_query="""
SELECT Id, Date, official_name_en AS Country, Type, AvgTone
FROM event INNER JOIN mappingcc ON event.Country = mappingcc.ISO
"""  # replace the country codes by country names
event = spark.sql(replace_country_code_by_name_query)

print("countries codes replaced by countries names, {} rows left in event".format(event.count()))
print("it is normal to lose them as some actors are not labelled by countries but by continent (AFR, ASA, EUR, ...))")


# read the mentions.csv files
mentions = spark.read.csv(os.path.join(DATA_DIR, "*.mentions.CSV"), sep="\t")
mentions = mentions.select(mentions._c0.cast("bigint").alias("EventId"),
	to_date(mentions._c2.substr(0,8), "yyyymmdd").alias("Date"),  # we do not take the chars correspodning to hours mins and seconds
	mentions._c4.alias("SourceName"),
	mentions._c11.cast("int").alias("Confidence"),
	mentions._c13.cast("double").alias("Tone"))

mentions = mentions.dropna(how='any')
mentions.registerTempTable('mentions')
print("mentions treated, {} rows left in mentions".format(mentions.count()))


# remove mentions that do not concern the events we kept
remove_mentions_query="""
SELECT *
FROM mentions
WHERE mentions.EventId IN (SELECT Id from event)
"""

mentions_cleaned = spark.sql(remove_mentions_query)
SQLContext(sc).dropTempTable('mentions')
mentions_cleaned.registerTempTable('mentions_cleaned')
#print("removing mentions about unknown events, {} rows left in mentions".format(mentions_cleaned.count()))


# read the mapping file and replace the url by their mappings in mentions
# data was taken from https://blog.gdeltproject.org/multilingual-source-country-crossreferencing-dataset/ on 30th october 2018
# mapping_url.Country was checked to be a subset of mapping_country_codes.official_name_en, there is only one entry
# that soesn't corresponds (pabsec.org	OS	Oceans), we were unable to find what that country is and we are thus ignoring that entry in the later
mapping_url = mapping_url.select(mapping_url.SourceCommonName.alias("url"), mapping_url._c2.alias("Country"))
mapping_url.registerTempTable('mappingurl')

replace_countries_query="""
SELECT EventId, Date, SourceName, Country, Confidence, Tone
FROM mentions_cleaned INNER JOIN mappingurl ON mentions_cleaned.SourceName = mappingurl.url
"""  # transforms urls into source countries
mentions_cleaned = spark.sql(replace_countries_query)

#print("mentions url_mapping, {} rows left in mentions".format(mentions_cleaned.count()))
print("it is normal that we lose some of them since the GDELT file doesn't contains all of the urls, see doc)")

# save as parquet
event.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_EVENT)
mentions_cleaned.write.mode('overwrite').parquet(PARQUET_FOLDER_NAME_MENTIONS)
print("data written in parquet files")