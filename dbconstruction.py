import psycopg2
import logging
import credentials as c

dbc_logger = logging.getLogger('freezam.dbconstruction')

def create_table():
    """A function to connect the local database and create our information
       tables for user's music inventory

    The first table SONGS will contain:
        + song_id PRIMARY KEY
        + song_title
        + artist_name

    The second table FINGERPRINTS will contain:
        + fingerprint_id PRIMARY KEY
        + song_id
        + window center
        + fingerprint1
        + fingerprint2

    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                        user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(""" CREATE TABLE IF NOT EXISTS songs(song_id SERIAL PRIMARY KEY,
                                                 song_title TEXT,
                                                 artist_name TEXT)""")
    conn.commit()
    dbc_logger.info("Successfully create Table songs/Table songs is already there!")
    cur.execute("""CREATE TABLE IF NOT EXISTS
               fingerprints(fingerprint_id SERIAL PRIMARY KEY,
                           song_id INTEGER REFERENCES songs(song_id) 
                           ON DELETE CASCADE,
                           window_center INTEGER,
                           fingerprint1 NUMERIC,
                           fingerprint2 NUMERIC ARRAY)""")
    dbc_logger.info("""Successfully create Table fingerprints/
                   Table fingerprints is already there!""")
    conn.commit()
    conn.close()
    dbc_logger.info("Done with initialization of database!")
    return
