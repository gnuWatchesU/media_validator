import subprocess
import sqlite3
import datetime
import logging
import os
import queue
import re
import rarfile
import transmissionrpc
import atexit
import extract
import torrentclient.transmission

MKV_MEDIA = {'mkv', 'mp2', 'mka', 'avi', 'wv', 'av1', 'mp4', 'ac3', 'h264', 'eac3ac3', 'ts', 'webm', 'mov', 'vob', 'ogv', 'rmvb', 'm2ts', 'drc', 'ivf', 'mpls', 'm1v', 'vc1', 'flac', 'mpeg', 'mk3d', 'rv', 'ogg', 'hevc', 'wav', 'x264', 'eac3', 'mts', 'ogm', 'ra', 'evo', 'rm', 'mks', 'caf', 'webma', 'tta', 'flv', 'mpg', 'aac', '264', 'obu', 'evob', 'ram', 'm4a', 'm2v', 'webmv', 'x265', 'h265', 'opus', 'mpv', 'm4v', '265', 'mp3', 'avc'}
MKV_SUBS = {'srt', 'ssa', 'ass', 'usf', 'idx', 'pgs', 'usf', 'xml', 'vtt', 'btn', 'sup', 'textst'}


class Inventory:
    conn = None
    c = None

    def __init__(self, db="~/.media_validator/inventory.db"):
        self.conn = sqlite3.connect(db, isolation_level=None)
        if self.conn:
            self.conn.row_factory = sqlite3.Row
            self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS files (path PRIMARY KEY, status, action, time);''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS dirs (path PRIMARY KEY, rar_path, subs_path, action, time);''')

    def __enter__(self):
        return self

    def lookup_file(self, filename):
        self.c.execute('select * from files where path = ?', (filename,))
        return self.c.fetchone()

    def lookup_dir(self, dirname):
        self.c.execute('select * from dirs where path = ?', (dirname,))
        return self.c.fetchone()

    def add_file(self, filename, status, action):
        if self.lookup_file(filename):
            self.c.execute("update files set status = ?, action = ?, time = ? where path = ?;", (status, action, datetime.datetime.now(), filename))
            logging.debug("Updating {} db file entry with: {} - {}".format(filename, status, action))
        else:
            self.c.execute("insert into files values (?,?,?,?)", (filename, status, action, datetime.datetime.now()))
            logging.debug("Adding {} db file entry with: {} - {}".format(filename, status, action))

    def add_dir(self, path, rar_path, subs_path, action):
        if self.lookup_dir(path):
            self.c.execute("update dirs set rar_path = ?, subs_path = ?, action = ?, time = ? where path = ?;", (rar_path, subs_path, action, datetime.datetime.now(), path))
            logging.debug("Updating {} db dir entry with: {}, {}, {}".format(path, rar_path, subs_path, action))
        else:
            self.c.execute("insert into dirs values (?,?,?,?,?)", (path, rar_path, subs_path, action, datetime.datetime.now()))
            logging.debug("Adding {} db dir entry with: {}, {}, {}".format(path, rar_path, subs_path, action))

    @atexit.register
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()

class MediaChecker:
    def __init__(self, config):
        logging.basicConfig(filename=config['log'], level=config['loglevel'], format='%(asctime)s - %(levelname)s: %(message)s')
        if config['printlog']:
            logging.getLogger().addHandler(logging.StreamHandler())
        logging.debug(config)
        self.db = Inventory(config['db_path'])
        self.fqueue = queue.Queue()
        self.dqueue = queue.Queue()
        self.config = config

        if self.config['torrent_client']['transmission']:
            try:
                self.transmission = torrentclient.transmission.TransmissionClient(
                    address=config['torrent_client']['transmission']['address'],
                    port=config['torrent_client']['transmission']['port'],
                    user=config['torrent_client']['transmission']['user'],
                    password=config['torrent_client']['transmission']['password'])
            except (ConnectionRefusedError, transmissionrpc.error.TransmissionError):
                logging.warning('Unable to connect to transmission client {}:{}'.format(
                    config['torrent_client']['transmission']['address'], config['torrent_client']['transmission']['port']))


    def __enter__(self):
        return self

    def _check_media(self, filename):
        logging.debug("Validating {}...".format(filename))
        try:
            status = subprocess.run(['ffmpeg', '-v', 'quiet', '-i', filename, '-f', 'null', '-'], timeout=900)
        except subprocess.TimeoutExpired:
            logging.error("Timed out waiting for {}".format(filename))
            return "timeout"
        if status.returncode is 0:
            return "OK"
        else:
            logging.warning("Failed to read {}".format(filename))
            return "invalid"

    def _ascertain_movie_name(self, ugly_name):
        ugly_name = ugly_name.replace('.', ' ')
        name_tokens = ugly_name.split()
        year_pattern = re.compile(r'^\(?(\d{4})\)?$')
        clean_name = ""
        for token in name_tokens:
            year_match = re.match(year_pattern, token)
            if year_match:
                clean_name+="({})".format(year_match.group(0))
                break
            else:
                clean_name+="{} ".format(token)
        return clean_name

    def _check_file(self, item):
        db_entry = self.db.lookup_file(item)
        if not self.config['force']:  # We don't care about the DB entry if we're forcing a recheck.
           if db_entry: # Short circuit if the entry isn't in the database
               if db_entry['action']:  # Ensure that we haven't done anything
                   logging.debug("{} has already been {} on {}".format(db_entry['path'], db_entry['action'],
                   db_entry['time']))
                   return
               elif db_entry['status'] == 'OK':  # Ensure the file isn't already OK.
                   logging.debug("{} has already been checked on {} and is OK".format(db_entry['path'], db_entry['time']))
                   return

        state = self._check_media(item)
        if state is "OK":
            logging.debug(item + " found to be valid.")
            self.db.add_file(item, state, None)
        else:
            if self.config['action'].lower() == 'delete':
                os.remove(item)
                logging.info("Deleting {}".format(item))
                self.db.add_file(item, state, "deleted")
            elif self.config['action'].lower() == 'move':
                dest_path = os.path.join(self.config['moveto'], os.path.split(item)[-1])
                os.rename(item, dest_path)
                logging.info("Moving {} to {}".format(item, dest_path))
                self.db.add_file(dest_path, state, "moved")
            else:
                logging.info(item + " found to be invalid.")
                self.db.add_file(item, state, None)

    def _check_dir(self, item):
        db_entry = self.db.lookup_dir(item)
        short_name = os.path.split(item)[-1]
        if not self.config['force']:  # We don't care about the DB entry if we're forcing a recheck.
            if db_entry:  # Short circuit if the entry isn't in the database
                if db_entry['action'] == 'merged':  # Make sure that we haven't done everything
                    logging.debug("{} has already been processed on {}".format(db_entry['path'], db_entry['time']))
                    return
        if self.config['decompress']:
            if self.transmission.find_torrent(short_name):
                logging.info("{} is currently being downloaded/seeded, leaving it alone.".format(short_name))
                return
            try:
                extract.recursive_unrar(os.path.join(db_entry['path'], db_entry['rar_path']))
            except rarfile.Error as err:
                logging.warning("Failed to expand archive {}: {}".format(item, err))
                self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'], 'error_decompress')
                return
            if 'rar' in db_entry['subs_path']:
                try:
                    extract.recursive_unrar(os.path.join(db_entry['path'], db_entry['subs_path']))
                except rarfile.Error as err:
                    logging.warning("Failed to expand subs {}: {}".format(item, err))
                    self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'],
                                    'error_subs')
                    return
                db_entry['subs_path'] = os.path.split(db_entry['subs_path'][0])
                self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'], 'decompressed')

            if self.config['merge']:
                nu_dir_contents = os.listdir(item)  # Directory contents have changed, let's get a fresh list
                nu_sub_contents = os.listdir(db_entry['subs_path'])
                parent_path, ugly_movie_name = os.path.splitext(item)
                pretty_movie_path = "{}/{}".format(parent_path, self._ascertain_movie_name(ugly_movie_name))
                try:
                    status = subprocess.run(['mkvmerge', '-o', pretty_movie_path] +
                                            list(MKV_MEDIA & set(nu_dir_contents)) +
                                            list(MKV_SUBS & set(nu_sub_contents)), timeout=900,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                except subprocess.TimeoutExpired:
                    logging.error("Took too long merging {}".format(pretty_movie_path))
                    self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'],
                                    'error_merge')
                except subprocess.CalledProcessError as err:
                    logging.error(
                        "Merging {} failed with: \n{}\n{}".format(pretty_movie_path, err.stderr, err.stdout))
                    self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'],
                                    'error_merge')
                self.db.add_dir(db_entry['path'], db_entry['rar_path'], db_entry['subs_path'], 'merged')

    def enqueue_path(self, path):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.startswith("._") or file is ".DS_Store":
                    os.remove(file)
                full_path = os.path.join(root, file)
                file_type = os.path.splitext(file)[1][1:].lower()
                if file_type in self.config['types']:
                    # self.db.add_file(full_path, None, None)  # Log it in the persistent db
                    self.fqueue.put(full_path)  # And add it to our short lived queue
                else:
                    logging.debug("Ignoring {} because the extension {} isn't checked".format(full_path, file_type))
            if self.config['decompress']:
                for d in dirs: # TODO: Can't deal with multiple RARs right now.
                    full_path = os.path.join(root, d)
                    dir_tuple = extract.find_rars(full_path)
                    if dir_tuple:
                        # self.db.add_dir(*dir_tuple)
                        self.dqueue.put(full_path)

    def process_queue(self, action):
        while not self.fqueue.empty():
            item = self.fqueue.get()
            self._check_file(item)
        while not self.dqueue.empty():
            item = self.dqueue.get()
            self._check_dir(item)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(None, None, None)
