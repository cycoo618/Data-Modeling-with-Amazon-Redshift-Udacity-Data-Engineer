import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist character varying(150),
        auth character varying(20) NOT NULL,
        firstName character varying(20),
        gender character varying (1),
        itemInSession double precision NOT NULL,
        lastName character varying(20),
        length double precision,
        level character varying(20) NOT NULL,
        location character varying(50),
        method character varying(20) NOT NULL,
        page character varying(20) NOT NULL,
        registration double precision,
        sessionId double precision NOT NULL,
        song character varying(200),
        status double precision NOT NULL,
        ts double precision NOT NULL,
        userAgent character varying(200),
        userId character varying(20) NOT NULL,
        PRIMARY KEY(userId, sessionId, itemInSession)
        )
""")
        
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs bigint IDENTITY(0,1) PRIMARY KEY,
        artist_id character varying(50) NOT NULL,
        artist_latitude character varying(50),
        artist_longitude character varying(50),
        artist_location character varying(200),
        artist_name character varying(200) NOT NULL,
        song_id character varying(50) NOT NULL,
        title character varying(200) NOT NULL,
        duration double precision NOT NULL,
        year numeric(4,0) NOT NULL
        )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id character varying(20) NOT NULL,
        level character varying(20) NOT NULL,
        song_id character varying(50) NOT NULL,
        artist_id character varying(50) NOT NULL,
        session_id double precision NOT NULL,
        location character varying(50),
        user_agent character varying(200)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id character varying(20) PRIMARY KEY,
        first_name character varying(20),
        last_name character varying(20),
        gender character varying (1),
        level character varying(20) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id character varying(50) PRIMARY KEY,
        title character varying(200) NOT NULL,
        artist_id character varying(50) NOT NULL,
        year numeric(4,0) NOT NULL,
        duration double precision NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id character varying(50) PRIMARY KEY,
        name character varying(200) NOT NULL,
        location character varying(200),
        latitude character varying(50),
        longitude character varying(50)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour numeric(2,0) NOT NULL,
        day numeric(2,0) NOT NULL,
        week numeric(2,0) NOT NULL,
        month numeric(2,0) NOT NULL,
        year numeric(4,0) NOT NULL,
        weekday numeric(1,0) NOT NULL
        );
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)


staging_songs_copy = ("""COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
        se.userId as user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionid as session_id, 
        se.location,
        se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss on se.artist = ss.artist_name and se.song = ss.title and se.length = ss.duration
    WHERE page = 'NextSong'

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time,
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(day FROM start_time) as day,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        EXTRACT(weekday FROM start_time) as weekday
    FROM songplays
""")

    
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
