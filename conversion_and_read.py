import os
import wave
import wavio
import logging
import numpy as np
# import matplotlib
# import matplotlib.pyplot as plt
from scipy import signal
from tinytag import TinyTag
from pydub import AudioSegment

cr_logger = logging.getLogger("freezam.conversion_and_read")

def SingleSong_conversion(file):
    """ Convert different formats of a single song to .wav format and store it
    in a different folder

    Parameter:
        + file (str): the local path of the stored music

    Returns:
        + new_name: the local path of music file in .wav format
        + music file to .wav format in the same directory
        + song_title (str): the metadata associated with song
        + artist_name (str): the metadata asssociated with song
        Or error message if invalid format of song is provided
    """
    try:
        song_title = TinyTag.get(file).title   
        artist_name = TinyTag.get(file).artist
        if song_title is None:
            song_title = os.path.basename(file).split(".")[0]
        if artist_name is None:
            artist_name = 'unknown'
        new_name = ''.join(file.split('.')[:-1])+'.wav'
        AudioSegment.from_file(file).export(new_name, format='wav')
        cr_logger.info("Successful Conversion! DONE!")
        return new_name, song_title, artist_name

    except Exception:
        cr_logger.error("Oops, invalid format found:"+ file)
        pass

def win_spectrogram(wav_path, window_method, window_size, window_shift):

    """ Read .wav file and return the spectrogram of the whole song
    Parameters:
        + wav_path (str): the local path of .wav file
        + window_method (str): the desired window method to generate
        the window data
        + window_size (int): the desired time length of window for chunking
        a song in time domain
        + window_shit (int): the fixed time gap between every two windows

    Return:
        + spectrogram (ndarray): the spectrogram of windowed data
        + frequency (ndarray): the frequency of windowed data
        + t (ndarray): the window centers of windowed data
        + plot: the spectrogram plot of the song
    """
    assert (os.path.splitext(wav_path)[1] == '.wav')  # must be a .wav file
    file = wave.open(wav_path, 'rb')
    # get the sampling rate of the music, usually 44100 hz
    sampling_rate = file.getframerate()
    # get the total number of samples in a song
    nframes = file.getnframes()
    # get te total number of channels
    nchannels = file.getnchannels()
    samplewidth = file.getsampwidth()
    # get the byte datq of a song
    data = file.readframes(nframes)
    file.close()

    # convert the byte data into ndarray
    array = wavio._wav2array(nchannels, samplewidth, data)
    # convert to mono channel
    mono_data = array.sum(axis=1) / 2
    # get the spectrogram of the whole song
    f, t, spec = signal.spectrogram(mono_data, fs=sampling_rate,
                                    window=window_method,
                                    nperseg=window_size*sampling_rate,
                                    noverlap=(window_size-window_shift)
                                    *sampling_rate)
    # plot the spectrogram
    # plt.pcolormesh(t, f, spec, norm = matplotlib.colors.Normalize(0,1))
    # plt.ylabel('Frequency [Hz]')
    # plt.xlabel('Time [sec]')
    # plt.show()
    
    cr_logger.info("Get spectrogram, frequency and time successfully!")
    return spec,f, t

def fingerprints_1(spec, f):

    """Calculating the one-dimensional summaries of the spectrogram of a song;
    Faster search but less accurate fingerprint.

    Parameter:
        + spectrogram: A ndarray that contains the spectrogram of windowed data
        + f: A ndarry that contains the frequencies of the song
        + t: A ndarray that contains the window center of the song

    Returns:
        + fingerprints: A ndarray that contains the one-dimensional summaries
        of a song
    """
    scale_parameter = f[np.argmax(f)]
    fingerprints = f[np.argmax(spec, axis=0)]/scale_parameter
    # plt.plot(t, fingerprints, color="crimson")
    cr_logger.info("Fingerprint 1 is successfully computed!")
    return fingerprints

def fingerprints_2 (spec, m, f):
    """ Calculating the m-dimensional summaries of the spectrogram of a song
    
    Parameter:
        + spec: A ndarray that contains the spectrogram of windowed data
        + m: Integer; The pre-specified number of interval in each window
        + f: A ndarray that contains the frequencies of the song
        
    Return:
        + fingerprints: A ndarray that contains the m-dimensional summaries
        of a song
    """
    f_nyq = 44100//2  # 44100 is the sampling frequency of a song
    start_pt = int((f_nyq/(2**(m+1)))*10) # frequency is the ten times of the index
    pecks_full = []
    for i in range(m):
        start = (2**i)*start_pt
        end = min((2**(i+1))*start_pt,len(spec))
        local_fingerprints = fingerprints_1(spec[start:end], f[start:end])
        pecks_full.append(local_fingerprints)
    
    cr_logger.info("Fingerprint 2 is successfully computed!")
    return np.array(pecks_full).T

def single_analyzer (file_path, window_method, window_size, window_shift, m):
    
    """The aggregate function to analyze a song. It calculates two different
    dimensional summarise of a single song. Fingerprint2 is more accurate than
    fingerprint1 because the alogrithm in fingerprint2 divides each window by m
    octaves and calculates the fingerprints within each octaves and finally 
    standardized the fingerprints within each octvaes.
    
    Parameters:
        + file_path (str): The physical location of a song in any valid music format
        supported by ffmepg
        + window_method (str): Method of interest to window the data
        + window_size (int): The desired time length of window for chunking
        a song in time domain
        + window_shift (int): The fixed time gap between every two windows
        + m (int): The pre-specified number of interval in each window
                 The required parameter for calculating fingerprint2
    
    Returns:
        + song_title (str): The metadata extracted from song itself
        + artist_name (str): The metadata extracted from song itself
        + fingerprint1 (ndarray): The one-dimensional summary of the entire song
        + fingerprint2 (ndarray): The m-dimensional summary of the entire song
        + t (ndarray): The window center of the music file
    """
    if file_path.split('.')[-1] == 'wav':
        song_title = file_path.split('/')[-1].split('.')[0]
        artist_name = 'unknown'
        spec,f, t = win_spectrogram(file_path, window_method, window_size, window_shift)
    else:
        file, song_title, artist_name = SingleSong_conversion(file_path)
        spec,f, t = win_spectrogram(file, window_method, window_size, window_shift)
        os.remove(file) # remove the converted file from user's current file
        cr_logger.info("The converted wav file has been removed")

    fingerprint1 = fingerprints_1(spec,f)
    fingerprint2 = fingerprints_2(spec, m, f) # an nxd numpy array
    
    cr_logger.info('Analyze done!')
    return song_title, artist_name, fingerprint1, fingerprint2, t
