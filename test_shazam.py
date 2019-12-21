## Run the tests by running
## pytest -v test_shazam.py
## All test functions must start with test_.

import os
import pytest
import psycopg2
import credentials as c
import conversion_and_read as cr 
import search_match as sm
import dbmanagement as dbm

def test_conversion():
    """ Here we will test if a song can be converted into .wav format
        successfully in the same directory

    """
    file = 'Sherlock OST/SHERlocked.mp3'
    new_name = cr.SingleSong_conversion(file)
    assert new_name[0].split('/')[-1] in os.listdir(os.path.split(file)[0])
    
def test_fingerprints():
    file = 'Sherlock OST/SHERlocked.wav'
    spec,f,t = cr.win_spectrogram(file,'hanning',10,1)
    fingerprint1 = cr.fingerprints_1(spec,f)
    assert fingerprint1.shape[0] == 215
    
    fingerprint2 = cr.fingerprints_2(spec,8,f)
    assert fingerprint2.shape == (215,8)

def test_add():
    """Here we will test if a song is successfully added in the database"""

    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(""" SELECT COUNT(*) FROM songs WHERE song_title = SHERlocked
                artist_name = unknown""")
    count = cur.fetchone()[0]
    assert count != 0

def test_duplicates():
    """Here we will test if remove_duplicates function work in the database """

    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(""" SELECT COUNT(CONCAT(song_title, ' ', artist_name)) 
                    FROM songs """)
    count1 = cur.fetchone()[0]
    cur.execute(""" SELECT COUNT(DISTINCT CONCAT(song_title, ' ', artist_name))
                    FROM songs """)
    count2 = cur.fetchone()[0]
    assert count1-count2 == 0

def test_delete():
    "Here we will test on the deletion of a song"
    dbm.delete('The Game Is On','David Arnold & Michael Price')
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(""" SELECT COUNT(CONCAT(song_title = 'The Game Is On', ' ', 
                                        artist_name = 'David Arnold & Michael Price')) 
                    FROM song """)
    count = cur.fetchone()[0]
    assert count == 0
    
def test_search_1():
    "Here we will test on search_1"
    assert 'SHERlocked' in sm.search_match_1(0.003458755)

def test_search_2():
    "Here we will test on search_2"
    s,a,f1,f2,t = cr.single_analyzer("The Game Is On.mp3","hanning",10,1,8)
    assert 'The Game Is On' in sm.search_match2(f2,3)

def test_lsh_search():
    "Here we will test on locality-sensitive hashing search"
    s,a,f1,f2,t = cr.single_analyzer("The Game Is On.mp3","hanning",10,1,8)
    centroid,query_obj = sm.setup()
    assert 'The Game Is On' in sm.lsh_search(query_obj,centroid,f2)
