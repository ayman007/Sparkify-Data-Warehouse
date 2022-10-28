import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                 (artist TEXT,auth TEXT,firstName TEXT,gender CHAR,itemInSession INTEGER,
                                 lastName TEXT,length FLOAT,level TEXT,location TEXT,method TEXT,page TEXT,
                                 registration FLOAT,sessionId INTEGER,song TEXT,status INTEGER,ts BIGINT,
                                 userAgent TEXT,userId varchar NOT NULL);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 (num_songs INTEGER,artist_id TEXT,artist_latitude float,artist_longitude float,
                                 artist_location TEXT,artist_name TEXT,song_id TEXT,title TEXT,duration FLOAT,
                                 year INTEGER);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (songplay_id INT IDENTITY(0,1) primary key,start_time timestamp NOT NULL,user_id varchar NOT NULL,
                            level varchar,song_id varchar,artist_id varchar,session_id int,
                            location varchar,user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id varchar NOT NULL primary key,first_name varchar,
                                                            last_name varchar,gender char,level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key,title varchar NOT NULL,
                                                            artist_id varchar,year int,duration float NOT NULL);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key,name varchar NOT NULL,
                                                                location varchar,latitude float,longitude float);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp primary key,hour int,day int,
                                                            week int,month int,year int ,weekday int);""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};""").format(config.get('S3','LOG_DATA'),
                                              config.get('IAM_ROLE','ARN'),
                                              config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';""").format(config.get('S3','SONG_DATA'),
                                                  config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id ,level , song_id ,
                                                    artist_id ,session_id ,location ,user_agent)
                                                    SELECT TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
                                                    e.userId,e.level,s.song_id,s.artist_id,e.sessionId,
                                                    e.location,e.userAgent
                                                    FROM staging_events e JOIN staging_songs s 
                                                    ON e.song = s.title AND e.artist = s.artist_name AND 
                                                    ABS(e.length - s.duration) < 2 
                                                    WHERE e.page = 'NextSong' """)

user_table_insert = ("""INSERT INTO users SELECT DISTINCT (userId) userId,
                        firstName, lastName, gender,level
                        FROM staging_events
                        where page='NextSong' """)

song_table_insert = ("""INSERT INTO songs SELECT DISTINCT (song_id) song_id,
                        title, artist_id, year, duration 
                        FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists SELECT DISTINCT (artist_id) artist_id,
                          artist_name, artist_location, artist_latitude, artist_longitude 
                          FROM staging_songs""")


time_table_insert = ("""INSERT INTO time WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts 
                        FROM staging_events)
                        SELECT DISTINCT ts,extract(hour from ts),extract(day from ts),extract(week from ts),
                        extract(month from ts),extract(year from ts),extract(weekday from ts)
                        FROM temp_time""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
