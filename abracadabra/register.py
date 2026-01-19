import os
import logging
from multiprocessing import Pool, Lock, current_process
from . import settings
from .fingerprint import fingerprint_file
from .storage import store_song, song_in_db, checkpoint_db
from .utils import get_song_info

KNOWN_EXTENSIONS = ["mp3", "wav", "flac", "m4a"]


def pool_init_global(l):
    """Init function that makes a lock available to each of the workers in
    the pool. Allows synchronisation of db writes since SQLite only supports
    one writer at a time.
    """
    global lock
    lock = l
    logging.info(f"Pool init in {current_process().name}")


def register_song(filename, info=None):
    """Register a single song.

    Checks if the song is already registered based on path provided and ignores
    those that are already registered.

    :param filename: Path to the file to register
    :param info: Song meta data. If None, this is extraced from the file ID3 tags.
                 If specified provide a tuple of (artist, albumartist, title)
    """
    if song_in_db(filename):
        logging.info(f"Song already in DB: {filename}")
        return
    hashes = fingerprint_file(filename)
    if info is not None:
        song_info = info
    else:
        song_info = get_song_info(filename)

    try:
        logging.info(f"{current_process().name} waiting to write {filename}")
        with lock:
            logging.info(f"{current_process().name} writing {filename}")
            store_song(hashes, song_info)
            logging.info(f"{current_process().name} wrote {filename}")
    except NameError:
        logging.info(f"Single-threaded write of {filename}")
        # running single-threaded, no lock needed
        store_song(hashes, song_info)


def register_directory(path):
    """Recursively register songs in a directory.

    Uses :data:`~abracadabra.settings.NUM_WORKERS` workers in a pool to register songs in a
    directory.

    :param path: Path of directory to register
    """
    logging.info(f"Registering directory:{path}")
    to_register = []
    for root, _, files in os.walk(path):
        for f in files:
            if f.split('.')[-1] not in KNOWN_EXTENSIONS:
                continue
            file_path = os.path.join(path, root, f)
            to_register.append(file_path)
    l = Lock()
    with Pool(settings.NUM_WORKERS, initializer=pool_init_global, initargs=(l,)) as p:
        p.map(register_song, to_register)
    # speed up future reads
    checkpoint_db()
