import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process json song file and insert into postgre SQL.
    :param cur: postgre sql cursor
    :param filepath: directory path where stored the song file
    :return: Void
    """
    # open song file
    df =pd.read_json(filepath,lines=True)

    # insert song record
    song_data =  (df['song_id'][0],df['title'][0],df['artist_id'][0],int(df['year'][0]),float(df['duration'][0])) 
    try:
        cur.execute(song_table_insert,song_data+song_data[1:])
    except psycopg2.Error as e:
        print("Error:Inserting song data")
        print(e)
    
    # insert artist record
    artist_data =  (df['artist_id'][0],df['artist_name'][0],df['artist_location'][0],float(df['artist_latitude'][0]),
                    float(df['artist_longitude'][0]))
    
    try:
        cur.execute(artist_table_insert, artist_data+artist_data[1:])
    except psycopg2.Error as e:
        print("Error:Inserting artist data")
        print(e)

def date_info_list(date_info):
    """
    - Returns a list with time data that will be used to create a   
      new dataframe
    :param date_info: Parse in datetime
    :return: Return a list of attribute populates time table
    """
    return [date_info.strftime('%Y-%m-%d %H:%M:%S'), date_info.hour, date_info.day, \
            date_info.isocalendar()[1], date_info.month, date_info.year, date_info.weekday()]

def date_info(date_info):
    """
    - Returns proper date format for func date_info_list to use
    :param date_info: datetime
    :return: datetime format output
    """
    return date_info.strftime('%Y-%m-%d %H:%M:%S')


def replaceEmptyInt(value):
    """
    - Used to handle when there is a int column with empty value
    :param value: data of int value column
    :return: None if it is empty, else return what ever the input.
    """
    if value == "":
        return None
    else:
        return value

def process_log_file(cur, filepath):
    import datetime
    # open log file
    df = pd.read_json(filepath,lines=True)
    # filter by NextSong action
    # convert timestamp column to datetime
   
    t=pd.to_datetime(df['ts'],unit='ms')   
    # insert time data records
    time_data = t.apply(date_info_list)
    column_labels = ('ts', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday') 
    time_df = pd.DataFrame(time_data.values.tolist(),columns=column_labels)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print ("Error: Inserting time data")
            print (e)


                # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]
    user_df =user_df[["userId","firstName","lastName","gender","level","firstName","lastName","gender","level"]].dropna()

                # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            print ("Error: Inserting user data")
            print (e)


    # insert songplay records
    for index, row in df.iterrows():     
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print ("Error: selecting song&artist id")
            print (e)
            
        results = cur.fetchone()        
        if results:
            songid, artistid = results
            print(results)
        else:
            songid, artistid = None, None
        
        start_time = datetime.datetime.fromtimestamp(int(row.ts)/1000).strftime("%Y-%m-%d %H:%M:%S")
        # insert songplay record
        songplay_data = (start_time,replaceEmptyInt(row.userId),row.level,songid,\
                         artistid,replaceEmptyInt(row.sessionId),row.location,row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data+songplay_data[2:])
        except psycopg2.Error as e:
            print ("Error: Inserting songplay data")
            print (e)

def process_data(cur, conn, filepath, func):
    """
    - Loop through source dir, call process_log_file/process_song_file
    function process each file respectively.
    :param cur: Postgre SQL cursor.
    :param conn: Postgre SQL connection.
    :param filepath: Source dir path.
    :param func: function wrapper.
    :return: void
    """
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

