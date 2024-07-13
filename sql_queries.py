import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession INT,
lastName VARCHAR,
length DOUBLE PRECISION,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id VARCHAR(255),
    num_songs INT,
    title VARCHAR(1024),
    artist_name VARCHAR(4096),
    artist_latitude DOUBLE PRECISION,
    year int,
    duration DOUBLE PRECISION,
    artist_id VARCHAR(255),
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(1024)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES song(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INT NOT NULL PRIMARY KEY,
first_name VARCHAR(255), 
last_name VARCHAR(255), 
gender VARCHAR(1), 
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(
song_id VARCHAR(255) PRIMARY KEY,
title VARCHAR(1024), 
artist_id VARCHAR(255), 
year INT, 
duration DOUBLE PRECISION
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR(255) PRIMARY KEY, 
name VARCHAR(256), 
latitude DOUBLE PRECISION, 
longitude DOUBLE PRECISION, 
location VARCHAR(1024)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY,
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday INT
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from '{}'
CREDENTIALS 'aws_iam_role={}'
format as json '{}'
region 'us-west-2'
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs from '{}'
CREDENTIALS 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays  (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, 
se.userid, 
se.level, 
ss.song_id, 
ss.artist_id, 
se.sessionid,
se.location, 
se.userAgent
from staging_events se
left join staging_songs ss
on se.artist = ss.artist_name 
and   se.song = ss.title
and  se.length = ss.duration
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userid as user_id, se.firstname as first_name, se.lastname as last_name, se.gender, se.level FROM
staging_events se
WHERE userid IS NOT NULL 
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration from staging_songs as ss
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, latitude, longitude, location)
SELECT DISTINCT artist_id, s.artist_name as name, s.artist_latitude as  latitude, s.artist_longitude as longitude, 
s.artist_location as location from staging_songs s
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts start_time, 
            EXTRACT(HOUR FROM ts) as HOUR, 
            EXTRACT(DAY FROM ts) as DAY, 
            EXTRACT(WEEK FROM ts) as WEEK, 
            EXTRACT(MONTH FROM ts) as MONTH, 
            EXTRACT(YEAR FROM ts) as YEAR, 
            EXTRACT(WEEKDAY FROM ts) as WEEKDAY
FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ' as ts FROM staging_events)
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
