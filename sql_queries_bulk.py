# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
song_data_temp_table_drop = "DROP TABLE IF EXISTS song_data_temp;"
log_data_temp_table_drop = "DROP TABLE IF EXISTS log_data_temp;"


# CREATE TABLES

song_data_temp_table_create = ("""CREATE TABLE IF NOT EXISTS song_data_temp 
(artist_id VARCHAR, artist_latitude VARCHAR, artist_location VARCHAR, artist_longitude VARCHAR, artist_name VARCHAR, duration VARCHAR, 
num_songs VARCHAR, song_id VARCHAR, title VARCHAR, year VARCHAR) ;
""")

log_data_temp_table_create = ("""CREATE TABLE IF NOT EXISTS log_data_temp 
(artist VARCHAR, auth VARCHAR, firstName VARCHAR, gender VARCHAR, itemInSession VARCHAR, 
lastName VARCHAR, length VARCHAR, level VARCHAR, 
location VARCHAR, method VARCHAR, page VARCHAR, registration VARCHAR, 
sessionId VARCHAR, song VARCHAR, status VARCHAR, ts VARCHAR, userAgent VARCHAR, userId VARCHAR) ;
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time BIGINT NOT NULL, user_id INT NOT NULL, level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id INT, location VARCHAR, user_agent VARCHAR)  ;
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender char(1), level VARCHAR NOT NULL, UNIQUE (user_id)) ;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR, year smallint, duration numeric, UNIQUE (song_id)) ;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, latitude real, longitude real, UNIQUE (artist_id)) ;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY, hour smallint, day smallint, week smallint, month smallint, year smallint, weekday smallint, UNIQUE (start_time)) ; 
""")

# INSERT RECORDS

copy_query_song = """COPY song_data_temp FROM %s DELIMITER ',' CSV HEADER;"""
copy_query_log = """COPY log_data_temp FROM %s DELIMITER ',' CSV HEADER;"""

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                    SELECT DISTINCT song_id, title, artist_id, CAST(year AS SMALLINT), CAST(duration AS NUMERIC) 
                    FROM song_data_temp
                    ON CONFLICT (song_id) DO NOTHING;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                    SELECT DISTINCT artist_id, artist_name, artist_location, CAST(artist_latitude AS REAL), CAST(artist_longitude AS REAL) 
                    FROM song_data_temp
                    ON CONFLICT (artist_id) DO NOTHING;
                    """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT CAST(userId AS INT), firstName, lastName, CAST(gender AS CHAR(1)), level
                        FROM log_data_temp
                        WHERE page = 'NextSong'
                        ON CONFLICT (user_id) DO NOTHING;
                    """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT CAST(ts AS BIGINT), 
                        CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT),
                        CAST(EXTRACT(DAY FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT),
                        CAST(EXTRACT(MONTH FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT),
                        CAST(EXTRACT(WEEK FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT),
                        CAST(EXTRACT(YEAR FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT),
                        CAST(EXTRACT(DOW FROM TO_TIMESTAMP(CAST(ts AS DOUBLE PRECISION) / 1000)) AS SMALLINT)
                        FROM log_data_temp
                        WHERE page = 'NextSong'
                        ON CONFLICT (start_time) DO NOTHING;
                    """)
                    
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                        SELECT CAST(L.ts AS BIGINT),
                        CAST(L.userId AS INT), 
                        L.level,
                        S.song_id,
                        A.artist_id, 
                        CAST(L.sessionId AS INT),
                        L.location, 
                        L.userAgent
                        FROM 
                        log_data_temp L LEFT JOIN songs S ON L.song = S.title AND CAST(L.length AS NUMERIC) = S.duration
                        LEFT JOIN artists A ON L.artist = A.name
                        WHERE page = 'NextSong';
                        """)

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_temp_table_queries = [song_data_temp_table_create, log_data_temp_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_temp_queries = [song_data_temp_table_drop, log_data_temp_table_drop]
