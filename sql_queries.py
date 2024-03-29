# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# SERIAL type defined for autogenerating values for songplay_id
songplay_table_create = (" \
        CREATE TABLE IF NOT EXISTS songplays ( \
            songplay_id SERIAL PRIMARY KEY, \
            start_time timestamp REFERENCES time(start_time), \
            user_id int REFERENCES users(user_id), \
            level varchar NOT NULL, \
            song_id varchar REFERENCES songs(song_id), \
            artist_id varchar REFERENCES artists(artist_id), \
            session_id int NOT NULL, \
            location varchar NOT NULL, \
            user_agent varchar NOT NULL \
            ) \
            ")

user_table_create = (" \
    CREATE TABLE IF NOT EXISTS users ( \
    user_id int PRIMARY KEY, \
    first_name varchar NOT NULL, \
    last_name varchar NOT NULL, \
    gender varchar NOT NULL, \
    level varchar NOT NULL\
    ) \
    ")

song_table_create = (" \
        CREATE TABLE IF NOT EXISTS songs ( \
        song_id varchar PRIMARY KEY, \
        title varchar NOT NULL, \
        artist_id varchar NOT NULL, \
        year int NOT NULL, \
        duration float NOT NULL \
        ) \
        ")

artist_table_create = (" \
        CREATE TABLE IF NOT EXISTS artists ( \
        artist_id varchar PRIMARY KEY, \
        artist_name varchar NOT NULL, \
        artist_location varchar NOT NULL, \
        latitude float, \
        longitude float \
        ) \
        ")

time_table_create = (" \
        CREATE TABLE IF NOT EXISTS time ( \
        start_time timestamp PRIMARY KEY, \
        hour int NOT NULL, \
        day int NOT NULL, \
        week int NOT NULL, \
        month int NOT NULL, \
        year int NOT NULL, \
        weekday int NOT NULL \
        ) \
        ")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays \
                          (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")

#user table has level details, which needs to be updated based on the latest timestamp's level data from logfile. To identify the latest level, the json files needs to be sorted. Within each json log file, records are already sorted by time.
user_table_insert = ("INSERT INTO users \
                      (user_id,first_name,last_name,gender,level) \
                       VALUES (%s,%s,%s,%s,%s)  \
                       ON CONFLICT (user_id) DO UPDATE \
                          SET level=EXCLUDED.level")

song_table_insert = ("INSERT INTO songs \
                      (song_id,title,artist_id,year,duration) \
                       VALUES (%s,%s,%s,%s,%s) \
                       ON CONFLICT DO NOTHING")

artist_table_insert = ("INSERT INTO artists \
                        (artist_id,artist_name,artist_location,latitude,longitude) \
                         VALUES (%s,%s,%s,%s,%s) \
                         ON CONFLICT DO NOTHING")

time_table_insert = ("INSERT INTO time \
                      (start_time,hour,day,week,month,year,weekday) \
                       VALUES (%s,%s,%s,%s,%s,%s,%s) \
                       ON CONFLICT DO NOTHING")

# FIND SONGS
song_select=("""SELECT songs.song_id,artists.artist_id \
                from songs join artists 
                on songs.artist_id=artists.artist_id 
                where artists.artist_name=%s and songs.duration=%s and songs.title=%s""")
    
# QUERY LISTS
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]