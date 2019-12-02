import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """ Processes all json files in the song data folder and loads data into songs and artists tables """
    
    # open song file
    df = pd.read_json(filepath,typ='series')

    # insert song record
    song_data=df.song_id,df.title,df.artist_id,df.year,df.duration    
    cur.execute(song_table_insert, song_data)   
    
    # insert artist record
    artist_data=df.artist_id,df.artist_name,df.artist_location,df.artist_latitude,df.artist_longitude
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """ Processes all log files in the log data folder and loads data into tables users, time, songplays """
    
    # open log file
    json_data = pd.read_json(filepath,lines=True)    
    
    # filter by NextSong action
    filtered_df=json_data[json_data['page']=='NextSong']

#Another Method of adding data to time table
#    for ind in filtered_df.index:
#        start_time=pd.to_datetime(filtered_df['ts'][ind], unit='ms')
#        time_data=start_time,start_time.hour,start_time.day,start_time.week,start_time.month,start_time.year,start_time.dayofweek
#        cur.execute(time_table_insert,time_data)
    
    time_df=filtered_df[['ts']]
    for i, row in time_df.iterrows():
        start_time=pd.to_datetime(row.ts, unit='ms')
        time_data=start_time,start_time.hour,start_time.day,start_time.week,start_time.month,start_time.year,start_time.dayofweek
        cur.execute(time_table_insert,time_data)        
    
    # load user table
    user_df=json_data[['userId','firstName','lastName','gender','level']]
    
    # insert user records    
    for i, row in user_df.iterrows():
        if row.userId.__class__==int: #There are some records which were in int type in json files
            user_data=row.userId,row.firstName,row.lastName,row.gender,row.level
            cur.execute(user_table_insert,user_data)
        elif row.userId.isdigit(): #This condition is for records which are in string data type and needs to be converted to int
            user_data=int(row.userId),row.firstName,row.lastName,row.gender,row.level
            cur.execute(user_table_insert,user_data)
                        
    # insert songplay records    
    notnull_df=json_data[json_data['artist'].notnull()&json_data['length'].notnull()&json_data['song'].notnull()]
    # above filters out records which has artist, length and song set to not null
    for index,row in notnull_df.iterrows(): 
        cur.execute(song_select,(row.artist,row.length,row.song))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None        
        
        # insert songplay record
        songplay_data=pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent
        cur.execute(songplay_table_insert, songplay_data)        
    
    
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate over files and process
    
    #user table has level details, which needs to be updated based on the latest timestamp's level data from logfile. To identify the latest level, the json files needs to be sorted. Within each json log file, records are already sorted by time.
    all_files.sort()
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
    print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()    
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()