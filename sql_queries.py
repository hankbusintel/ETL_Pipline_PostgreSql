# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays 
            (  
               songplay_id serial not null primary key, 
               start_time timestamp,
               user_id int, 
               level varchar, 
               song_id varchar,
               artist_id varchar,
               session_id int,
               location varchar,
               user_agent varchar,
               UNIQUE(start_time,user_id)
            )
            
""")

user_table_create = ("""
         CREATE TABLE IF NOT EXISTS users 
            (  
               user_id int, 
               first_name varchar, 
               last_name varchar,
               gender varchar,
               level varchar,
               PRIMARY KEY(user_id)
            )
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs 
            (  
               song_id varchar, 
               title varchar, 
               artist_id varchar,
               year int,
               duration numeric,
               PRIMARY KEY(song_id)
            )
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists  
            (  
               artist_id varchar, 
               name varchar, 
               location varchar,
               latitude numeric,
               longitude numeric,
               PRIMARY KEY(artist_id)
            )
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time  
            (  
               start_time timestamp, 
               hour int, 
               day int,
               week int,
               month int,
               year int,
               weekday int,
               PRIMARY KEY(start_time)
            )
""")

# INSERT RECORDS

songplay_table_insert = ("""
     INSERT INTO songplays 
     ( start_time,user_id,level,song_id,artist_id,session_id,location, user_agent) 
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(start_time,user_id) DO UPDATE SET 
     level=%s,song_id=%s,artist_id=%s,session_id=%s,location=%s,user_agent=%s
""")

user_table_insert = ("""
    INSERT INTO users 
    (user_id, first_name,last_name,gender,level)
     VALUES (%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET first_name=%s,last_name=%s,gender=%s,level=%s
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s,%s,%s,%s,%s) ON CONFLICT(song_id) DO UPDATE SET title=%s,artist_id=%s,year=%s,duration=%s
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (artist_id, name, location, latitude, longitude)
    VALUES (%s,%s,%s,%s,%s) ON CONFLICT(artist_id) DO UPDATE SET name=%s,location=%s,latitude=%s,longitude=%s
""")


time_table_insert = ("""
    INSERT INTO time 
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(start_time) DO NOTHING
""")

# FIND SONGS
song_select = ("""
    
    SELECT s.song_id,a.artist_id
    FROM songs s
    INNER JOIN artists a
    ON s.artist_id = a.artist_id
    WHERE s.title = %s
    AND a.name = %s
    AND s.duration = %s


""")

song_select1 = ("""
    SELECT * FROM songs
""")
song_play_select= ("""
    SELECT * FROM songplays

""")
user_select= ("""
    SELECT * FROM users
""")
artist_select= ("""
    SELECT * FROM artists 
""")
time_select= ("""
    SELECT * FROM time 
""")

# QUERY LISTS
# In real world development, we should also create foreign key for songplay table as well.
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
select_table_queries={
    'songplays':song_play_select,'songs':song_select1,'getid':song_select,'users':user_select,'artists':artist_select,'time':time_select
}