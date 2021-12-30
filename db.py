import sqlite3
import logging
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

class Inventory(object):
    conn = None
    c = None

    def __init__(self, config):
        engine = create_engine(config['db_path'])

        logger = logging.getLogger(__name__)
        setup_logger(config, logger)

        # self.conn = sqlite3.connect(config['db_path'], isolation_level=None)
        # if self.conn:
        #     self.conn.row_factory = sqlite3.Row
        #     self.c = self.conn.cursor()
        # self.c.execute('''CREATE TABLE IF NOT EXISTS files (path PRIMARY KEY, status, action, time);''')
        # self.c.execute('''CREATE TABLE IF NOT EXISTS dirs (path PRIMARY KEY, rar_path, subs_path, action, time);''')

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
            self.c.execute("update files set status = ?, action = ?, time = ? where path = ?;", (status, action, datetime.now(), filename))
            logging.debug("Updating {} db file entry with: {} - {}".format(filename, status, action))
        else:
            self.c.execute("insert into files values (?,?,?,?)", (filename, status, action, datetime.now()))
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
