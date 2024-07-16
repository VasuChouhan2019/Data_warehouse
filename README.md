### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

Here database source is Sparkify application, where it stored data in json format for the songs and events. We have created a star schema with facts and dimension table that can help us to identify business insight easy and quick. Which we can use to identify popularity of songs, artist and many more things. This is necessary as on you cant simply get information directly from S3 and join the data which is in JSON format. Star Schema make it wasy. We can further connect it powerBI or amazon quicksight to make dashboard. 


### State and justify your database schema design and ETL pipeline.
I use Star Schema here, where fact table is SongPlays and dimension tables are user, song, artist, time.

Things which took me time to figure out and I guess its important
1. We need to use LEFT JOIN to get right amount of data while making Songsplay
2. Create Dimension table first so that it will be easy to make fact table and provide references. 
3. I just simply using VARCHAR while importing small set from S3 and when then exploring manually to set datatype and import full dataset in right datatype.

### Description of the scripts

#### 1. dwh.cfg
This is credentials data. Contain AWS Secret keys and Access keys for IAM user, which we can use to generate a Redshift cluster via Pyhton as IaaS . This needs to be noted - It should have role attached due to which it can access S3 data from where data is stored and second it should have security group attached so that we can access it and create and copy table and copt data.

It also contain links to S3, where out data is stored. 

#### 2. sql_queries.py
This script contain all the queries, which include Creating table in Redshift, Copying data in those table from S3 and Inserting data from Staging to Star Schema. We will  them use these SQL commands in create_table.py and etl.py after importing these.

#### 3. create_tables.py 
This script will first connect with Redshift cluster using credentials from dwh.cfg and then create table. We also have drop table command just to be on safe side so that no errors comes up while creating those tables, if table already exist.

#### 4. etl.py
This script will first connect with Redshift cluster using credentials from dwh.cfg then copy data in table we created by create_tables.py from S3 data. After that inserting same data from staging table to fact and dimesnion table.

We always needs to use copy command for copying data from S3 to Redshift as it is fast. Once data is there we can use insert to have it copy in staar schema.
