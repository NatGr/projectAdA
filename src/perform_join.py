import os

def perform_join(spark):
	"""
	performs the join between the events and the mentions table by using spark
	return the joined table after having registered a corresponding TempTable with the same name (joined_table)
	"""
	DATA_DIR = "hdfs:///user/greffe"
	PARQUET_FOLDER_NAME_EVENT = os.path.join(DATA_DIR, "Event.parquet")
	PARQUET_FOLDER_NAME_MENTIONS = os.path.join(DATA_DIR, "Mentions.parquet")

	events = spark.read.parquet(PARQUET_FOLDER_NAME_EVENT)
	mentions = spark.read.parquet(PARQUET_FOLDER_NAME_MENTIONS)
	events.registerTempTable('events')
	mentions.registerTempTable('mentions')

	join_query = """
	SELECT events.Id, events.Date AS EventDate, events.Country AS ActorCountry, events.Type AS ActorType, 
	mentions.Date AS MentionDate, mentions.SourceName AS SourceName, mentions.Country AS MentionCountry, 
	mentions.Confidence AS Confidence, mentions.Tone AS Tone
	FROM mentions INNER JOIN events ON events.Id = mentions.EventId
	"""  # join the two tables and group it by the actor and the mention country

	joined_table = spark.sql(join_query)
	joined_table.registerTempTable('joined_table')
	return joined_table