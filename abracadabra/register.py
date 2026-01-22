import os
import logging
from multiprocessing import Pool
from . import settings
from .fingerprint import fingerprint_file
from .storage import store_song, store_songs, song_in_db, get_cursor
from .utils import get_song_info

KNOWN_EXTENSIONS = ["mp3", "wav", "flac", "m4a"]


def _fingerprint_worker(filename):
    """Worker for pool that fingerprints a file."""
    if song_in_db(filename):
        logging.info(f"Song already in DB: {filename}")
        return None
    hashes = fingerprint_file(filename)
    song_info = get_song_info(filename)
    return hashes, song_info


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

    with get_cursor() as (conn, c):
        batch = []
        with Pool(settings.NUM_WORKERS) as p:
            for result in p.imap_unordered(_fingerprint_worker, to_register):
                if result:
                    batch.append(result)
                    if len(batch) >= 100:
                        store_songs(batch, conn=conn)
                        batch = []
            if batch:
                store_songs(batch, conn=conn)
