import subprocess
import sqlite3
import datetime
import logging
import os
import queue

class Inventory:
    conn = None
    c = None

    def __init__(self, db="~/.media_validator/inventory.db"):
        self.conn = sqlite3.connect(db)
        if self.conn:
            self.conn.row_factory = sqlite3.Row
            self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS files (path PRIMARY KEY, status, action, time);''')

    def __enter__(self):
        return self

    def lookup(self, filename):
        self.c.execute('select * from files where path = ?', (filename,))
        return self.c.fetchone()

    def log(self, filename, status, action):
        if self.lookup(filename):
            self.c.execute("update files set status = ?, action = ?, time = ? where path = ?;", (status, action, datetime.datetime.now(), filename))
        else:
            self.c.execute("insert into files values (?,?,?,?)", (filename, status, action, datetime.datetime.now()))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()

class MediaChecker:
    def __init__(self, config):
        logging.basicConfig(filename=config['log'], level=config['loglevel'], format='%(asctime)s - %(levelname)s: %(message)s')
        logging.debug(config)
        self.db = Inventory(config['db_path'])
        self.queue = queue.Queue()
        self.config = config

    def __enter__(self):
        return self

    def _check_media(self, filename):
        try:
            status = subprocess.run(['ffmpeg', '-v', 'quiet', '-i', filename, '-f', 'null', '-'], timeout=300)
        except TimeoutError:
            logging.error("Timed out waiting for {}".format(filename))
            return "timeout"
        if status.returncode is 0:
            return "OK"
        else:
            logging.warning("Failed to read {}".format(filename))
            return "invalid"

    def enqueue_path(self, path):
        for root, _, files in os.walk(path):
            for file in files:
                full_path = "{}/{}".format(root, file)
                file_type = os.path.splitext(file)[1][1:].lower()
                if file_type in self.config['types']:
                    self.db.log(full_path, None, None)  # Log it in the persistent db
                    self.queue.put(full_path)  # And add it to our short lived queue
                else:
                    logging.debug("Ignoring {} because the extension {} isn't checked".format(full_path, file_type))

    def process_queue(self, action):
        while not self.queue.empty():
            item = self.queue.get()
            db_entry = self.db.lookup(item)
            if db_entry and db_entry['action'] or db_entry['status'] is 'OK':
                logging.debug("{} has already been processed on {}".format(db_entry['path'], db_entry['time']))
            else:
                state = self._check_media(item)
                if state is "OK":
                    logging.debug(item + " found to be valid.")
                    self.db.log(item, state, None)
                else:
                    if action is 'delete':
                        # os.remote(item)
                        logging.info("Deleting {}".format(item))
                        self.db.log(item, state, "deleted")
                    else:
                        logging.info(item + " found to be invalid.")
                        self.db.log(item, state, None)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(None, None, None)
