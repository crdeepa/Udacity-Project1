# Data Modeling with Postgres!
> A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. This project extracts, processes the data which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app and loads them into fact and dimensions tables.

## General info
    Song files data are loaded into the tables - songs and artitsts.
    Log files data are loadde into the tables - users, time and songplays.

## Below are the structures of the tables created
    (.tables.PNG)

## Songs and Artists table
    The data for songs and artists tables are directly loaded from the songs files avoiding any duplicates
    
## User table
    The users table data is loaded from the logfiles information. But, in case of duplicates, the level column is updated based on the latest timestamp. Since user table do not have a column to capture timestamp, the logfiles are read in order of the names. Within each logfile, the records are already sorted.
    
## Time table
    The time table contains the information of all times in the logfiles after removing the duplicates. 
    
## Songplays table
    The songplays table is the fact table used in the database. It has columns referencing to all the other 4 dimensions table created above.