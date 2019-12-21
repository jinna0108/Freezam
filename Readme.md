# Freezam 
> A Simplified Version of Song Matching

## Freezam Introduction

Freezam is a simplified version of Shazam. It takes the exact 10-second snippet and applies fourier transforms, signal processing, locality-sensitive hashing to find the best possible matched songs in the current database with decent accuracy in a fast speed. The details of how Freezam works are explained as below.

### Reading Audio Data

We convert different types of music files into wav format in order to easily get access to the time series data in the music files. We decode the time series data into 2*(sampling frequency*time) numpy array. Since most of music files are stero channels, we take the average of left and right channel to make our data a one-dimensional numpy array, which simplifies the signal structure over time. 

### Windowing

Analyzing the snippets requires us to truncate the songs into a reasonable number of overlapping windows. The default window size is 10s and the default window shift is 1s in this app. Our strategy is to use a window function that approximates a Gaussian, because the Gaussian gives the best concentration over time and frequency. Thus, the weights put on each signal point of each window can be chosen between Hanning or Blackman window function. 

### Fast Fourier Transformation

After acquiring the windowed data, FFT is used to convert data from time-frequency structure into frequency-amplititude structure for every window. 

### Signature Extraction

Since it is extremely inefficient to store and compare with the full local periodgrams of a song, we consider signatures – lower-dimensional summaries of the local periodograms – as alternative keys for the comparison. We developed two strategies to extract the low-dimensional signatures:

1. A list of (positive) frequencies f (scaled to [0, 1]) at which the local periodogram has a
peak. The one-dimensional signature will be computed for each window. This function is named as `fingerprints_1` under `conversion_and_read` module.

2. A vector (p_k) where each p_k is the maximum amplititude in the local periodogram (smoothed
or unsmoothed) within the kth among m pre-specified, non-overlapping frequency
intervals (such as successive octaves, frequency bands doubling/halving in width, down
to some minimum frequency 2^−(m+1)*f_Nyq). The signature picked in each frequency interval will be divided by p_k in to be rescaled to [0,1). This function is given as `fingerprints_2` under `conversion_and_read` module.

### Database Schema

To efficiently manage the database, we arrange the data into two tables, songs and fingerprints. The schema of the database is represented as below:

| Table Songs     |                    |                 |
|-----------------|--------------------|-----------------|
| Song_id (PK)    | Song_title         | artist_name     |
| 1               | title1             | name1           |
| 2               | title2             | name2           |
| ...             | ...                | ...             |

| Table Fingerprints  |                   |                       |                        |                              |
|---------------------|-------------------|-----------------------|------------------------|------------------------------|
| fingerprint_id (PK) | song_id (FK)      | window_center         | fingerprint1 (NUMERIC) | fingerprint2 (NUMERIC ARRAY) | 
| 1                   |                 1 |                     5 | 0.004363567            | [0.905462,0.836512,...]      |
| 2                   |                 1 |                     6 | 0.001897562            | [0.752125,0.967854,...]      |
| ...                 |               ... |                   ... | ...                    | ...                          |

### Hashing

Locality-sensitive hashing provided by `falconn` package is applied in the query process. Locality-sensitive hashing can boost up the query speed of the near neighbour searching of high-dimensional signatures.

### Matching

Freezan provid different matching strategies.

1. Rough search. This will calculate the absolute distance between snippet's one-dimensional fingerprint with fingerprint1 stored in the database. The numbers of absolute distances less than the pre-specified tolerance level will be counted for each songs stored in the database. The best possible matches will be songs with maximum number of tolerable distances. This function is named as `search_match_1` in search_match module.

2. Slow search. This takes the high-dimensional signature and apply the method of K-Nearest-Neighbour alogrithm (borrowed from `KNeighborsClassifier` package from `sklearn.neighbors` module) to find the best possible matches. This provides more accurate searching results. This function is named as `search_match2` in search_match module.

3. Fast search. This is the improved version of slow search, which aims to improve the query speed of near neighbour searching of high-dimensional signatures. `setup` function in search_match module is used to set up the LSH table for later processing. `lsh_search` in search_match modeule is used to find the best possible songs under a pre-specified threshold. If no song is matched within the pre-specified threshold, this function will throw a message.

## How To Interact With Freezam
> Freezam provides with 5 main functions
> 1. Push music inventory into database
> 2. Add a song to the current database
> 3. Remove a song from the current database
> 4. Identify a snippet with the current databse
> 5. Clean the duplicates in database

### Push music inventory into database

```
python main.py push_all PATH_OF_DIRECTORY
```

### Add a song to the current database

```
python main.py add PATH_OF_SONG --title "song_title" --artist "artist_name"
```

### Remove a song from the current database

```
python main.py delete --title "song_title" --artist "artist_name"
```

### Identify a snippet with the current databse

1. Rough search

```
python main.py identify PATH_OF_SNIPPET --search 1
```

2. Slow search

```
python main.py identify PATH_OF_SNIPPET --search 2
```

3. Fast search

```
python main.py identify PATH_OF_SNIPPET --search 3
```

### Clean the duplicates in database

```
python main.py rm_duplicate
```

## Running the tests

Run the tests by running:

```
pytest -v test_shazam.py 
```

We tested on the **file conversion**, **fingerprint calculation**, **searching process**, and **add, delete and remove duplicate** functions of Freezam.

## Limitations

The computation of fingerprints of songs is not perfect enough and the tolerance level of each seaching algorithms needs to be tuned in the future. This system can only take exact 10s snippet, which is another limitation. 

## Authors

* **Na Jin** - *A learner of programming language.* *This is my first python project*
