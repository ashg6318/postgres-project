# Sparkify Database: Postgres

This project details the _ETL pipeline_ for the **Sparkify Database**. We also provide a few sample queries using the Songplays, Songs, Users, Time and Artists tables.

There are TWO ways of ingestion are demonstrated: 
1. Bulk Insertion using `COPY <table> FROM _file_`
2. Line by Line Insertion using `INSERT INTO <table> VALUES ()` 
    - if you don't have permissions to do a bulk insert
    - at least 5-10X slower than bulk insertion


## Files and Folders

### data

Contains all the data needed to populate the tables in the sparkify database
- songs_data : individual song information 
- log_data : activity from user sessions


### sql_queries.py / sql_queries_bulk.py

These file contains all the relevant postgresql queries needed to create and drop all the tables in the database
Use `sql_queries_bulk.py` for bulk insertion instead of `sql_queries.py`


### create_tables.py

This files creates the sparkify database and all it's tables. Each create is preceded by a dropping of all tables to start afresh. This step may be skipped if we have a stable operating database


### etl.py

This file executes the ETL pipeline of:
1. Extracting data from raw json files
2. Transforming it to tabular (Relational Ddatabase) format
3. Loading into the relevant tables

The ETL process uses both the song and log files present in `data/song_data` and `data/log_data`

Time taken for the whole process prints as STDOUT


### etl_bulk.py

This file executes the ETL pipeline of:
1. Extracting data from raw json files and storing in a local csv file
2. Loading into the temporary staging tables
3. Transforming data in the staging tables and loading data into the the fact and dimension tables

This process uses both the song and log files present in `data/song_data` and `data/log_data`

Time taken for the whole process prints as STDOUT


## Setting up the Database

1. Open Terminal. Change directory to `/home/workspace` if not done already
2. Run `sql_queries.py` for data insertion line by line or `sql_queries_bulk.py` for bulk insertion
3. Once `sql_queries` runs without errors, run `create_tables.py`
4. If `create_tables.py` runs without errors, run `etl.py` for data insertion line by line or `etl_bulk.py` for bulk insertion

Running `etl.py` or `etl_bulk.py`successfully populates the songs, artists, time, users and songplays tables


## Schemas

### songplays

    | Column Name  | Data Type |  Example |
    | ------------- | ------------- | ------------- | 
    | songplay_id  | SERIAL PRIMARY KEY  | 1  |
    | start_time  | BIGINT  | 1543449657796  |
    | user_id  | INT  | 73  |
    | level  | VARCHAR  | paid  |
    | song_id  | VARCHAR  | SOZCTXZ12AB0182364  |
    | artist_id  | VARCHAR  | AR5KOSW1187FB35FF4  |
    | session_id  | INT  | 954  |
    | location  | VARCHAR  | Tampa-St. Petersburg-Clearwater, FL  |
    | user_agent  | VARCHAR  | "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"  |

### songs
    
    | Column Name  | Data Type |  Example |
    | ------------- | ------------- | ------------- | 
    | song_id  | VARCHAR PRIMARY KEY  | SOINLJW12A8C13314C  |
    | title  | VARCHAR  | City Slickers  |
    | artist_id  | VARCHAR  | AR8IEZO1187B99055E  |
    | year  | SMALLINT  | 2008  |
    | duration  | NUMERIC  | 149.86404  |

### artists
    
    | Column Name  | Data Type |  Example |
    | ------------- | ------------- | ------------- | 
    | artist_id  | VARCHAR PRIMARY KEY  | AR0IAWL1187B9A96D0  |
    | name  | VARCHAR  | Danilo Perez  |
    | location  | VARCHAR  | Panama  |
    | latitude  | REAL  | 8.4177  |
    | longitude  | REAL  | -80.1128  |
    
### users
    
    | Column Name  | Data Type |  Example |
    | ------------- | ------------- | ------------- | 
    | user_id  | INT PRIMARY KEY  | 73  |
    | first_name  | VARCHAR  | Jacob  |
    | last_name  | VARCHAR  | Klein  |
    | gender  | CHAR(1)  | M  |
    | level  | VARCHAR  | paid  |

### time
    
    | Column Name  | Data Type |  Example |
    | ------------- | ------------- | ------------- | 
    | start_time  | BIGINT PRIMARY KEY  | 1543449657796   |
    | hour  | SMALLINT  | 0 |
    | day  | SMALLINT  | 29  |
    | week  | SMALLINT  | 48  |
    | month  | SMALLINT  | 11  |
    | year  | SMALLINT  | 2018  |
    | weekday  | SMALLINT  | 3  |

## Sample Queries

- Paid Songplay counts by weekday : ```SELECT weekday, COUNT(*) 
                                  FROM songplays JOIN time 
                                  ON songplays.start_tim e = time.start_time
                                  WHERE songplays.level = 'paid'
                                  GROUP BY weekday
                                  ORDER BY weekday; ```
                                  
- Songplay counts by gender : ```SELECT gender, COUNT(*) 
                                  FROM songplays JOIN users 
                                  ON songplays.user_id = users.user_id
                                  GROUP BY gender
                                  ORDER BY gender; ```        