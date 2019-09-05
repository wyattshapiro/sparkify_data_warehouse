import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS app_user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR, \
                                                                            auth VARCHAR, \
                                                                            firstName VARCHAR, \
                                                                            gender VARCHAR, \
                                                                            itemInSession INTEGER, \
                                                                            lastName VARCHAR, \
                                                                            length DECIMAL, \
                                                                            level VARCHAR, \
                                                                            location VARCHAR, \
                                                                            method VARCHAR, \
                                                                            page VARCHAR, \
                                                                            registration BIGINT, \
                                                                            sessionId INTEGER, \
                                                                            song VARCHAR, \
                                                                            status INTEGER, \
                                                                            ts BIGINT, \
                                                                            userAgent VARCHAR, \
                                                                            userId VARCHAR);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (song_id VARCHAR PRIMARY KEY, \
                                                                           num_songs INTEGER, \
                                                                           artist_id VARCHAR, \
                                                                           artist_latitude DECIMAL, \
                                                                           artist_longitude DECIMAL, \
                                                                           artist_location VARCHAR, \
                                                                           artist_name VARCHAR, \
                                                                           title VARCHAR, \
                                                                           duration DECIMAL, \
                                                                           year INTEGER);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, \
                                                                 start_time TIMESTAMP NOT NULL, \
                                                                 user_id VARCHAR NOT NULL, \
                                                                 level VARCHAR, \
                                                                 song_id VARCHAR, \
                                                                 artist_id VARCHAR, \
                                                                 session_id VARCHAR, \
                                                                 location VARCHAR, \
                                                                 user_agent VARCHAR);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS app_user (user_id VARCHAR PRIMARY KEY, \
                                                             first_name VARCHAR, \
                                                             last_name VARCHAR, \
                                                             gender VARCHAR, \
                                                             level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id VARCHAR PRIMARY KEY, \
                                                         title VARCHAR, \
                                                         artist_id VARCHAR NOT NULL, \
                                                         year INTEGER, \
                                                         duration DECIMAL);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id VARCHAR PRIMARY KEY, \
                                                             name VARCHAR, \
                                                             location VARCHAR, \
                                                             latitude DECIMAL, \
                                                             longitude DECIMAL);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, \
                                                         hour INTEGER, \
                                                         day INTEGER, \
                                                         week INTEGER, \
                                                         month INTEGER, \
                                                         year INTEGER, \
                                                         weekday VARCHAR);""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM '{}'
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json '{}';""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         credentials 'aws_iam_role={}'
                         region 'us-west-2'
                         format as json 'auto';""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            SELECT
                                TIMESTAMP 'epoch' + event.ts/1000 * interval '1 second' AS start_time, \
                                event.userId, event.level, song.song_id, song.artist_id, \
                                event.sessionId, song.artist_location, event.userAgent \
                            FROM staging_events event \
                            JOIN staging_songs song ON \
                                event.song = song.title \
                                AND event.artist = song.artist_name \
                                AND event.length = song.duration
                            WHERE event.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO app_user (user_id, first_name, last_name, gender, level) \
                        SELECT DISTINCT userId, firstName, lastName, gender, level \
                        FROM staging_events \
                        ;""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT song_id, title, artist_id, year, duration \
                        FROM staging_songs \
                        ;""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude) \
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude \
                          FROM staging_songs \
                          ;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        SELECT
                            DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                            EXTRACT(hour FROM start_time),
                            EXTRACT(day FROM start_time),
                            EXTRACT(week FROM start_time),
                            EXTRACT(month FROM start_time),
                            EXTRACT(year FROM start_time),
                            EXTRACT(weekday FROM start_time) \
                        FROM staging_events \
                        ;""")

# ANALYZE TABLES

artist_play_count_all_time = ("""SELECT artist.name, COUNT(songplay.songplay_id) as songplay_count \
                                 FROM songplay \
                                 JOIN artist ON songplay.artist_id = artist.artist_id \
                                 GROUP BY artist.name \
                                 ORDER BY songplay_count DESC
                                 LIMIT 5;""")

song_play_count_all_time = ("""SELECT song.title, COUNT(songplay.songplay_id) as songplay_count \
                               FROM songplay \
                               JOIN song ON songplay.song_id = song.song_id \
                               GROUP BY song.title \
                               ORDER BY songplay_count DESC
                               LIMIT 5;""")

user_by_level = ("""SELECT level as user_level, COUNT(user_id) as user_count \
                    FROM app_user \
                    GROUP BY level \
                    ORDER BY user_count DESC;""")

song_play_count_by_user_level = ("""SELECT app_user.level as user_level, COUNT(songplay.songplay_id) as songplay_count \
                                    FROM songplay \
                                    JOIN app_user ON songplay.user_id = app_user.user_id \
                                    GROUP BY app_user.level \
                                    ORDER BY songplay_count DESC;""")

song_play_count_by_location = ("""SELECT location as songplay_location, COUNT(songplay_id) as songplay_count \
                                  FROM songplay \
                                  GROUP BY location \
                                  ORDER BY songplay_count DESC \
                                  LIMIT 5;""")

song_play_count_daily_by_hour = ("""SELECT time.hour as hour_of_day, COUNT(songplay.songplay_id) as songplay_count \
                                    FROM songplay \
                                    JOIN time ON songplay.start_time = time.start_time \
                                    GROUP BY time.hour \
                                    ORDER BY songplay_count DESC \
                                    LIMIT 5;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
analyze_table_queries = [artist_play_count_all_time, song_play_count_all_time, user_by_level, song_play_count_by_user_level, song_play_count_by_location, song_play_count_daily_by_hour]
