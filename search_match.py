import falconn # set up locality-sensitive hashing
import logging
import psycopg2 # database connection
import numpy as np 
import credentials as c # database username and password
from functools import reduce
from sklearn.neighbors import KNeighborsClassifier

sm_logger = logging.getLogger('freezam.search_match')

def retriv_fgp1(i, snip_fgp1):
    """Retrive the fingerprint1 from database, clean the database output
    properly and then calculate the distance between snippet fingerprint1 and
    the whole song fingerprint1
    
    Parameters:
        + i (int): The index of the song in database
        + snip_fgp1 (numeric): The fingerprint1 of the snippet
    
    Returns:
        A list that contain the absolute distance between snip_fgp1 and fgp1
        of the song with song_id i in the database
    """
    
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    sql_command = "SELECT fingerprint1 FROM fingerprints WHERE song_id = %s"
    cur.execute(sql_command, [i])
    fgp1 = cur.fetchall()
    conn.close()
    
    fgp1 = reduce(np.append, fgp1)
    for i in range(len(fgp1)):
        fgp1[i] = float(fgp1[i])
    distance1 = abs(snip_fgp1-fgp1)
    sm_logger.info("Distance has been calculated")
    return distance1   
    
def retriv_name(i):
    
    """ Rretrive the name of the matched song
    
    Parameter:
        i: the song_id 
        
    Return:
        The matched song name in the database
    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    sql_command = "SELECT song_title FROM songs WHERE song_id = %s"
    cur.execute(sql_command, [i])
    name = cur.fetchall()
    conn.close()
    name = reduce(np.append, name[0])
    sm_logger.info("Retrived name successfully")
    return name
    
# slow search of using one-dimensional fingerprints
def search_match_1(snip_fgp1):

    """ A function used to search through the whole database of 
    signatures and find the best possible matches 
    
    Parameters:
        snippet_fingerprint: the one-dimensional summary of the snippet 
        provided by user
    
    Return:
        The best possible matches of song titles of the snippet provided by the 
        user within the prespecified tolerance level
    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT song_id FROM songs")
    uniq_id = cur.fetchall()
    uniq_id = reduce(np.append, uniq_id)
    
    tolerance = 10**(-3)  # this is the default tolerance level, tuned
    
    matching_cnt = []
    window_num = []
    
    for song_id in uniq_id:
        distance = retriv_fgp1(int(song_id), snip_fgp1)
        matching_cnt.append(np.sum(distance<=tolerance))
        window_num.append(len(distance))
    
    # This is the new criterion: must have more than 10% similarity of a song
    # in the database - considered different lengths of songs
    similarity_lst = list(map(lambda i,j: i/j > 0.1, matching_cnt, window_num))
    matched_idx = [i for i,val in enumerate(similarity_lst) if val==True]
    matched_sid = [uniq_id[i] for i in matched_idx]
    
    if matched_sid == []:
        sm_logger.info('Oops, we try hard but find nothing...')
        return None
    else:
        possible_lst = []
        for i in matched_sid:
            possible_lst.append(retriv_name(int(i)))
        sm_logger.info('Found some songs matched the snippet!')
        return possible_lst

# slow search of using high-dimensional fingerprints
def search_match2(snip_fgp2, k):
    
    """ Using K-nearest neighbour alogrithm to search through the whole 
    database of multi-dimensional signatures and find the best possible matches
    
    Parameters:
        snip_fgp2: multi-dimensional signatures
        k: prespecified number of neighbour for K-nearest neighbour alogrithm
    
    Return:
        The best possible matches of song titles of the snippet provided by the user within
        the prespecified tolerance level
    """

    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT fingerprint2 FROM fingerprints")
    fgp2 = cur.fetchall()
    cur.execute("SELECT song_id FROM fingerprints")
    song_id = cur.fetchall()
    sid_cleaned = reduce(np.append, song_id)
    fgp2_cleaned = []
    
    for l in range(len(fgp2)):
        n = reduce(np.append, fgp2[l])
        fgp2_cleaned.append([float(n[ii]) for ii in range(len(n))])

    knn = KNeighborsClassifier(n_neighbors=k, metric='euclidean')
    knn.fit(fgp2_cleaned, sid_cleaned)
    predict = knn.predict(snip_fgp2.reshape(1,-1))
    song_name = retriv_name(int(predict[0]))

    return song_name

# fast search of using high-dimensional fingerprints
def setup():

    """ A function used to set up the Locality-Sensitive Hashing for 
    high-dimensional fingerprints. This function will extract the fingerprint2
    from database and make these fingerprints as nxd nparray. The nxd array
    will be used as the parameters to set up the LSH table.

    Returns:
        + centroid: An nparray that contains the mean of each column in
        fingerprint2 matrix
        + lsh_tbl:  The constructed LSH table used for later on query
    """
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT fingerprint2 FROM fingerprints")
    fingerprint2 = cur.fetchall()
    conn.close()

    data = np.array([tup[0] for tup in fingerprint2])
    data = data.astype(np.float32)
    centroid = np.mean(data, axis=0) # learned from the author of falconn
    data -= centroid  # make sense since each column represents octave band; trick
                      # provide by the author of falconn
    parameters = falconn.get_default_parameters(num_points=data.shape[0],
                                            dimension=data.shape[1])
    lsh_tbl= falconn.LSHIndex(parameters)
    lsh_tbl.setup(data)
    return centroid, lsh_tbl.construct_query_object()

def lsh_search(query_obj,centroid,snip_fingerprint2):

    """ A function used to find the best possible matches of a snippet
    by using Locality-Sensitive Hashing on high-dimensional fingerprints

    Parameters:
        + query_obj: The query object created by falconn package
        + centroid: An nparray that contains the mean of each column in
        fingerprint2 matrix from the current database
        + snip_fingerprint2: An nparray that contains snippet fingerprint2,
        the high-dimensional fingerprints
    
    Returns:
        + The id, title and window center of the best possible matched songs
        within the default threshold
        + Notification of finding nothing if nothing is found within the 
        tolerance level
    """
    possible_matches = []
    fingerprint2 = snip_fingerprint2[0]  
    fingerprint2 = fingerprint2.astype(np.float32)
    distance = fingerprint2-centroid
    tolerance_level = 10**(-2)
    possible_matches.append(query_obj.find_near_neighbors(distance, tolerance_level))
    
    conn = psycopg2.connect(host="sculptor.stat.cmu.edu", database=c.DB_USER,
                            user=c.DB_USER, password=c.DB_PASSWORD)
    cur = conn.cursor()
    matched_songs = []
    for id in possible_matches[0]:
        sql_cmd = """SELECT f.song_id, s.song_title, f.window_center
                     FROM fingerprints f JOIN songs s ON f.song_id = s.song_id
                     WHERE f.fingerprint_ID = %s"""
        cur.execute(sql_cmd, [id])
        matched_songs += cur.fetchall()
    conn.close()
    
    if len(matched_songs) != 0:
        sm_logger.info("Matched songs found")
        print(matched_songs)
        return matched_songs
    else:
        sm_logger.info("404 not found")
        print("We tried hard but found nothing in current inventory")
        return None
