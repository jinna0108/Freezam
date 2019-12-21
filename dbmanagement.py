import os
import psycopg2
import logging
import credentials as c
import conversion_and_read as cr 

dbm_logger = logging.getLogger("freezam.dbmanagement")

def add(song_title, artist_name, fingerprint1, fingerprint2, t):

    """ A function to add a new song with user-defined name into the
        current database

    Parameters:
        + song_title (str): user-defined song title
        + artist_name (str): user-defined artist name
        + window_size (int): the time length of the window
        + shift (int): the fixed time gap between every two windows
        + fingerprint1 (ndarray): A ndarray that contains one-dimensional summaries
        of a song
        + fingerprint2 (ndarray): A ndarray that contains m-dimensional summaries
        of a song
        + t (ndarray): A ndarray that contains the window centers of a song

    Returns:
        Add a song and its following information into database
    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    sql_command = " INSERT INTO songs (song_title, artist_name) VALUES (%s,%s) "

    cur.execute(sql_command, (song_title, artist_name))
    cur.execute("SELECT MAX(song_id) FROM songs")
    new_id = cur.fetchone()[0]
    
    for i in range(len(t)):
        sql_command = """INSERT INTO fingerprints (window_center, fingerprint1, 
                         fingerprint2, song_id) VALUES (%s,%s,%s,%s)"""
        cur.execute(sql_command, (t[i], fingerprint1[i],
                                  fingerprint2[i].tolist(), new_id))
    conn.commit()
    conn.close()
    dbm_logger.info('Sent the song to the database as requested!')
    return

def push_all(directory, window_size, shift, window_method, m):

    """ A function to push all music inventory into database

    Parameters:
        + directory: the directory of music files
        + window_size: the desired time length of window for chunking a song
        in time domain
        + shift: the fixed time gap between every two windows
        + window_method (str): the desired window method to generate window data
        + m (int): the prespecified m for calculating fingerprint2

    Return:
        Push all music in the inventory into database with necessary
        information

    """
    os.chdir(directory)
    for file in os.listdir(directory):
        try:
            song_title, artist_name, fingerprint1, fingerprint2, t = cr.single_analyzer(file, 
                                            window_method, window_size, shift, m)
            if song_title is None:
                song_title = file.split('.')[0]
                
            if artist_name is None:
                artist_name = 'unknown'
            add(song_title, artist_name, fingerprint1, fingerprint2, t)    
        except:
            pass
    dbm_logger.info("""All songs within this directory have been successfully pushed
                to database!""")
    return

def remove_duplicates():
    """ This function is used to clean the duplicates from the database """

    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("""DELETE
                   FROM
                       songs a
                         USING songs b
                    WHERE a.song_id < b.song_id AND
                          a.song_title = b.song_title AND
                          a.artist_name = b.artist_name""")
    conn.commit()
    conn.close()
    dbm_logger.info('Duplicate(s) has/have been removed successfully!')
    return

def delete(stitle, artname):

    """ A function used to remove a song from the current database

    Parameters:
        + stitle (str): the song title intended to drop
        + artname (str): the artist name of the song intended to drop

    Return:
        Remove a song in the current database
    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    sql_command = """ DELETE FROM songs WHERE song_title = %s AND
                    artist_name = %s """
    cur.execute(sql_command, (stitle, artname))
    conn.commit()
    conn.close()
    dbm_logger.info('Delete Successfully!')
    return
