import uuid
import logging
import mysql.connector
from mysql.connector import pooling
from collections import defaultdict
from contextlib import contextmanager
import csv
import os
import tempfile
from . import settings



_db_pool = None


@contextmanager
def get_cursor():
    """Get a connection/cursor to the database.

    :returns: Tuple of connection and cursor.
    """
    global _db_pool
    conn = None
    try:
        if _db_pool is None:
            config = settings.DB_CONFIG.copy()
            config['allow_local_infile'] = True
            _db_pool = pooling.MySQLConnectionPool(
                pool_name="abracadabra_pool",
                pool_size=5,
                **config
            )
        conn = _db_pool.get_connection()
        yield conn, conn.cursor()
    except Exception as error:
        logging.error(f"Exception during mysql connection: {error}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception as error:
            logging.error(f"Exception during mysql close: {error}")


def setup_db():
    """Create the database and tables.

    To be run once through an interactive shell.
    """
    with get_cursor() as (conn, c):
        c.execute("CREATE TABLE IF NOT EXISTS hash (hash BIGINT, offset FLOAT, song_id VARCHAR(255))")
        c.execute("CREATE TABLE IF NOT EXISTS song_info (artist VARCHAR(255), album VARCHAR(255), title VARCHAR(255), song_id VARCHAR(255))")
        # dramatically speed up recognition
        try:
            c.execute("CREATE INDEX idx_hash ON hash (hash)")
        except mysql.connector.Error as err:
            if err.errno != 1061:  # Duplicate key name
                raise


def song_in_db(filename):
    """Check whether a path has already been registered.

    :param filename: The path to check.
    :returns: Whether the path exists in the database yet.
    :rtype: bool
    """
    with get_cursor() as (conn, c):
        song_id = str(uuid.uuid5(uuid.NAMESPACE_OID, filename).int)
        c.execute("SELECT * FROM song_info WHERE song_id=%s", (song_id,))
        return c.fetchone() is not None


def store_song(hashes, song_info, conn=None):
    """Register a song in the database.

    :param hashes: A list of tuples of the form (hash, time offset, song_id) as returned by
        :func:`~abracadabra.fingerprint.fingerprint_file`.
    :param song_info: A tuple of form (artist, album, title) describing the song.
    :param conn: Optional connection to use.
    """
    if len(hashes) < 1:
        # TODO: After experiments have run, change this to raise error
        # Probably should re-run the peaks finding with higher efficiency
        # or maybe widen the target zone
        return

    if conn is None:
        with get_cursor() as (conn, c):
            store_song(hashes, song_info, conn)
        return

    with conn.cursor() as c:
        c.executemany("INSERT INTO hash VALUES (%s, %s, %s)", hashes)
        insert_info = [i if i is not None else "Unknown" for i in song_info]
        c.execute("INSERT INTO song_info VALUES (%s, %s, %s, %s)", (*insert_info, hashes[0][2]))
        conn.commit()


def store_songs(songs, conn=None):
    """Register multiple songs in the database.

    :param songs: A list of tuples of the form (hashes, song_info).
    :param conn: Optional connection to use.
    """
    all_hashes = []
    all_infos = []
    for hashes, song_info in songs:
        if len(hashes) < 1:
            continue
        all_hashes.extend(hashes)
        insert_info = [i if i is not None else "Unknown" for i in song_info]
        all_infos.append((*insert_info, hashes[0][2]))

    if len(all_hashes) < 1:
        return

    if conn is None:
        with get_cursor() as (conn, c):
            store_songs(songs, conn)
        return

    # Create temporary files for bulk loading
    fd_h, path_h = tempfile.mkstemp()
    fd_i, path_i = tempfile.mkstemp()

    try:
        with os.fdopen(fd_h, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t', lineterminator='\n')
            writer.writerows(all_hashes)

        with os.fdopen(fd_i, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t', lineterminator='\n')
            writer.writerows(all_infos)

        with conn.cursor() as c:
            # Use forward slashes for paths to ensure compatibility
            c.execute(f"LOAD DATA LOCAL INFILE '{path_h.replace(os.sep, '/')}' INTO TABLE hash FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n' (hash, offset, song_id)")
            c.execute(f"LOAD DATA LOCAL INFILE '{path_i.replace(os.sep, '/')}' INTO TABLE song_info FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n' (artist, album, title, song_id)")
            conn.commit()

    except Exception as e:
        logging.error(f"Error during bulk load: {e}")
        raise
    finally:
        # Clean up temp files
        if os.path.exists(path_h):
            os.remove(path_h)
        if os.path.exists(path_i):
            os.remove(path_i)


def get_matches(hashes, threshold=5):
    """Get matching songs for a set of hashes.

    :param hashes: A list of hashes as returned by
        :func:`~abracadabra.fingerprint.fingerprint_file`.
    :param threshold: Return songs that have more than ``threshold`` matches.
    :returns: A dictionary mapping ``song_id`` to a list of time offset tuples. The tuples are of
        the form (result offset, original hash offset).
    :rtype: dict(str: list(tuple(float, float)))
    """
    h_dict = {}
    for h, t, _ in hashes:
        h_dict[h] = t
    in_values = f"({','.join([str(h[0]) for h in hashes])})"
    with get_cursor() as (conn, c):
        c.execute(f"SELECT hash, offset, song_id FROM hash WHERE hash IN {in_values}")
        results = c.fetchall()
    result_dict = defaultdict(list)
    for r in results:
        result_dict[r[2]].append((r[1], h_dict[r[0]]))
    return result_dict


def get_info_for_song_id(song_id):
    """Lookup song information for a given ID."""
    with get_cursor() as (conn, c):
        c.execute("SELECT artist, album, title FROM song_info WHERE song_id = %s", (song_id,))
        return c.fetchone()
