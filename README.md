# What does the world think of us?
# Abstract
<!-- A 150 word description of the project idea, goals, dataset used. What story you would like to tell and why? What's the motivation behind your project? -->

The goal of this project is to asses the tone of news reports of an event caused by a country (or some of its citizens) and to use this information to rank countries on how they are seen abroad. We could see its evolution over time and the metric could be compared with the tone national news are reported inside of the country itself.  
We want to use the GDELT dataset (tables *export* and *mentions*) to get the tone used by news of country A to talk about events caused by country B.  
The goal and motivation of the project would be to identify countries that are liked abroad (this would be especially interesting if some of these countries are poor) so as to show good examples of behaviour. 

# Research questions
<!-- A list of research questions you would like to address during the project. --> 

	- What is the reputation of countries seen from abroad ?
	- Is this vision different from the image the national news give of the country ?
	- Is the built estimator statistically reliable (amount of samples) ?
	- Do the scores vary a lot when we change the assessing methodology (using the confidence attribute 
	of the GDELT database, mean of the opinions vs median opinion) ?
	- Giving news from "bad" countries less weight to compute the "goodness"
	 of a country (example: Saudi Arabia has bad reputation so don't care so much 
	 that Saudi Arabia criticizes Canada). 
	- Using the *Actor1Type1Code* field to identify the type of actor
	 the event refers to (ex: government, civilians,...) and refine the score of a 
	 country in different scores. This will also reduce the amount of available data 
	 since the *Actor1Type1Code* field is often blank.
	 - Showing differences between the opinions of different mediums inside of  acountry

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
		- MentionTimeDate: in case we want to sort out news based on older events
		- MentionSourceName: to get the country of the source, we can use the [GDELT mapping from urls to nationnality](https://blog.gdeltproject.org/multilingual-source-country-crossreferencing-dataset/). 
		- Confidence: in case we try to weight the importance of a news based on the confidence GDELT 
		has he computed the metadata correctly
		- MentionDocTone: the tone used in the article.

Regarding the data size, the total dataset weight is of 112GB but it is mostly composed of the GDELT graph which we won't use. After performing the above mentionned data selecting/cleaning, the size of the parquet files were of 0.83 and 3.09 GB (12GB on disc). We thus decided to download a subset of these locally to test our scripts and then to perform the analysis on the real data on the cluster.

# A list of internal milestones up until project milestone 3
<!-- Add here a sketch of your planning for the next project milestone. -->

11.11 :

 - ~Clean and organize our data in dataframes, if the size of the resulting dataframes is not too important, download locally.~
 - ~Categorize countries according to their reputation~
 - ~Compare with the vision from the national news~

18.11 : 

 - ~trying different approaches for assesing scores~
 - ~Analyse statistical reliability of the results~
 - ~Analyse results (what to keep, ...)~

25.11 :

 - ~Completing the notebook~
 - ~Goals and plans for the next milestone~

-----
02/12:
- use the different types of actors of a country to compute several ranks and compare these between countries with confidence intervals.  
- interpret inner and outer view scatterplot (maybe relate these with PIB, continent, wether the country is seen as a democracy by the occidental world,...)
- trying to group countries in different clusters from the country_to_country view

06/12:  
- compare the average tone of small versus big medias to see if there is a difference  
- try to get a global idea about inner-media variance inside of a country
16/12:
- once we have decided what to keep, what to say and how to order it, designing the data story (animations, beautiful plots on javascript)  

-----

    
15/01:
- deciding what to put on the poster  
21/01:
- poster creation and presentation repetition  
    
The presentation should be organised as follow (subject to change):
1. Global results
2. inner vs outer view interpretation
3. clustering


# Questions for TAa
<!-- Add here some questions you have for us, in general or project-specific. -->

 - Once we tested a script locally on a subset of the data, how do we make it run on the cluster?
 - If we want to retreive medium sized files (~1GB) on our computers (so as to be able to perform the rest of the analysis locally), are we supposed to download it over ssh?

-------

1: "The georeferenced location for an actor may not always match the
Actor1\_CountryCode or Actor2\_CountryCode field, such as in a case where the President of Russia is visiting Washington, DC in the United States, in which case the Actor1\_CountryCode would contain the code for Russia, while the georeferencing fields below would contain a match for Washington, DC.", see [docs](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)