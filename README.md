# What does the world think of us?
# Abstract
<!-- A 150 word description of the project idea, goals, dataset used. What story you would like to tell and why? What's the motivation behind your project? -->

The goal of this project is to asses the tone of news reports of an event caused by a country (or some of its citizens) and to use this information to rank countries on how they are seen abroad. We could see its evolution over time and the metric could be compared with the tone national news are reported inside of the country itself.  
We want to use the GDELT dataset (tables *export* and *mentions*) to get the tone used by people of country A to talk about event caused by country B.  
The goal and motivation of the project would be to identify countries that are liked abroad (this would be especially interesting if some of these countries are poor) so as to show good examples for other countries. 

# Research questions
<!-- A list of research questions you would like to address during the project. --> 

	- What is the reputation of countries seen from abroad ?
	- Is this vision different from the image the national news give of the country ?
	- Is the built estimator statistically reliable (amount of samples) ?
	- Do the scores vary a lot when we change the assessing methodology (using the confidence of the 
	GDELT database, mean of the opinions vs median opinion) ?
	- (If time permits) giving news from "bad" countries less weight to compute the "goodness"
	 of a country (example: Saudi Arabia has bad reputation so don't care so much 
	 that Saudi Arabia criticizing Canada). 
	- (If time permits) using the *Actor1Type1Code* field to identify the type of actor
	 the event refers to (ex: government, civilians,...) and refine the score of a 
	 country in different scores. This will also reduce the amount of available data 
	 since the *Actor1Type1Code* field is often blank.

# Dataset
<!-- List the dataset(s) you want to use, and some ideas on how do you expect to get, manage, process and enrich it/them. Show us you've read the docs and some examples, and you've a clear idea on what to expect. Discuss data size and format if relevant. -->

We want to use the GDELT dataset. More precisely we are interested in:  

	- event table (export.csv):
		- GlobalEventID: the id of the event
		- MonthYear: the month and year in which the event occured
		- Actor1CountryCode: Actor1CountryCode is more reliable to determine the first actor 
		nationality than Actor1Geo_CountryCode <sup>1</sup>.
		- Actor1Type1Code: the type of the actor, to potentially refine the score
		- AvgTone: if we want to perform some source-independent ranking.
	- mentions table (mentions.csv):
		- GlobalEventID: the id of the event
		- MentionTimeDate: in case we want to sort news based on older events
		- MentionSourceName: to get the country of the source, we can use the [GDELT mapping from urls to nationnality](https://blog.gdeltproject.org/multilingual-source-country-crossreferencing-dataset/). 
		- Confidence: in case we try to weight the importance of a news based on the confidence GDELT 
		has he computed the metadata correctly
		- MentionDocTone: the tone used in the article.

# A list of internal milestones up until project milestone 2
<!-- Add here a sketch of your planning for the next project milestone. -->

# Questions for TAa
<!-- Add here some questions you have for us, in general or project-specific. -->



-------

1: "The georeferenced location for an actor may not always match the
Actor1\_CountryCode or Actor2\_CountryCode field, such as in a case where the President of Russia is visiting Washington, DC in the United States, in which case the Actor1\_CountryCode would contain the code for Russia, while the georeferencing fields below would contain a match for Washington, DC.", see [docs](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)