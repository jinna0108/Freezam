import argparse  # used for designing user-friendly interface
import logging   # used for setting up logging file
import sys
import dbmanagement as dbm
import dbconstruction as dbc
import conversion_and_read as cr
import search_match as sm


# Here will be the user interface design


# some commandline shoud look like:


# freezam add --title="Song title" --artist="Artist name" song.mp3
# freezam identify snippet.mp3
# freezam identify snippet.wav
# freezam --help

# Set Up logging
logger = logging.getLogger('freezam')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('freezam.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


# Set Up Interface
parser = argparse.ArgumentParser(description='Welcome to FreeZam !',
                                 epilog='Enjoy Freezam :-)')
parser.add_argument("-vb","--verbose", action = "store_true", help = "change the log levels")

""" Several functions will be provided
+ Push music inventory into database
+ Add a song to the current database
+ Remove a song from the current database
+ Identify a snippet with the current databse
+ Clean the duplicates in database

"""
subparsers = parser.add_subparsers(dest='subcommands')

# create the parser for the "push" command
push_parser = subparsers.add_parser('push_all', help="""push music inventory
                                    with fingerprints into database""")
push_parser.add_argument('music_directory', type = str, help="""the physical location
                          of music inventory""")

# create the parser for the "add" command
add_parser = subparsers.add_parser('add', help="""add a song to
                                    the current database""")
add_parser.add_argument('file_path', type = str, help="""the physical location of
                         the song of interest""")
add_parser.add_argument('--title', "-t", help='the name of the song of interest')
add_parser.add_argument('--artist', "-a", help="""the artist of the song
                         of interest""")

# create the parser for the "identify" command
identify_parser = subparsers.add_parser('identify', help="""identify the
                                         snippet with the current database""")
identify_parser.add_argument('snippet', type = str, help="the snippet you want to match")
identify_parser.add_argument('--search', "-s", default=3, choices = [1,2,3], type = int,
                              help="""two searching options:
                              1 - rough search with one-dimensional signatures; 
                              2 - slow search with multi-dimensional signatures
                              3 - LSH search with multi-dimensional signatures""" )
# should be str b/c user input is recognized as str
# create the parser for the "delete" command
delete_parser = subparsers.add_parser('delete', help="""remove a song
                                       from the current database""")
delete_parser.add_argument('--title', "-t", help='the name of the song intended to delete')
delete_parser.add_argument('--artist', "-a", help="""the artist of the song
                         intended to delete""")

# create the parser for the "rm_duplicate" command
rm_duplicate_parser = subparsers.add_parser('rm_duplicate', help="""clean the 
                                            duplicate of songs in the database""")

args = parser.parse_args()

if args.verbose:
    ch.setLevel(logging.DEBUG)
    print("Verbosity is turned on")
else:
    ch.setLevel(logging.WARNING)

if args.subcommands == 'push_all':
    
    window_size = 10
    shift = 1
    window_method = 'hanning'
    m = 8
    
    dbc.create_table()
    dbm.push_all(args.music_directory, window_size, shift, window_method, m)
    
if args.subcommands == 'add':
    
    window_size = 10
    shift = 1
    window_method = 'hanning'
    m = 8
    
    song_title, artist_name, fingerprint1, fingerprint2, t = cr.single_analyzer(args.file_path, 
                                                        window_method, window_size, shift, m)
    # show all arguments into a dict called vars
    var = vars(parser.parse_args())
    if var["title"] is None and var["artist"] is None:
        dbm.add(song_title, artist_name, fingerprint1, fingerprint2, t)
    if var["title"] is not None and var["artist"] is not None:
        dbm.add(args.song_title, args.artist_name, fingerprint1, fingerprint2, t)
    if var["title"] is not None and var["artist"] is None:
        dbm.add(args.song_title, artist_name, fingerprint1, fingerprint2, t)
    if var["title"] is None and var["artist"] is not None:
        dbm.add(song_title, args.artist_name, fingerprint1, fingerprint2, t)
    print("Sent the song to database successfully!")

if args.subcommands == 'rm_duplicate':
    dbm.remove_duplicates()
    print("Database is very neat now!")
    
if args.subcommands == 'identify':
    window_size = 10
    shift = 1
    window_method = 'hanning'
    m = 8
    
    song_title, artist_name, fingerprint1, fingerprint2, t = cr.single_analyzer(args.snippet, 
                                                        window_method, window_size, shift, m)
    var = vars(parser.parse_args())
    if var["search"] == 2:
        print(sm.search_match2(fingerprint2,3))
    elif var["search"] == 1:
        print(sm.search_match_1(fingerprint1))
    else:
        centroid, query_obj = sm.setup()
        sm.lsh_search(query_obj,centroid,fingerprint2)

if args.subcommands == 'delete':
    try:
        dbm.delete(args.title, args.artist)
        print("Delete the song successfully!")
    except:
        print("Oops, something went wrong...")
