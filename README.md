#### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

Here I am assuming database source is Sparkify application, where it stored data in json format for the songs and events. We have created a star schema with facts and dimension table that can help us to identify business insight easy and quick. Which we can use to identify popularity of songs, artist and many more things. This is necessary as on you cant simply get information directly from S3 and join the data which is in JSON format. Star Schema make it wasy. We can further connect it powerBI or amazon quicksight to make dashboard. 


#### State and justify your database schema design and ETL pipeline.
I use Star Schema here, where fact table is SongPlays and dimension tables are user, song, artist, time.

Things which took me time to figure out and I guess its important
1. We need to use LEFT JOIN to get right amount of data while making Songsplay
2. Create Dimension table first so that it will be easy to make fact table and provide references. 
3. I just simply using VARCHAR while importing small set from S3 and when then exploring manually to set datatype and import full dataset in right datatype
