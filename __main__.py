import json
import logging

import integrity
import argparse
import os
import sys
import shutil

from db import Inventory


def main():
    parser = argparse.ArgumentParser(description="Recursively check if media is valid.")
    parser.add_argument('path', help="Where to look")
    parser.add_argument('--loglevel',
                        help='Log Level: currently supports debug, info, error, and fatal.  Defaults to info.')
    parser.add_argument('--printlog', help='Also print logs to stderr', action='store_true')
    parser.add_argument('--action',
                        help='Action to perform on failed validations.  Currently supports delete, none, and move.  Defaults to none.')
    parser.add_argument('--moveto', help='Move bad files to this directory.  Only useful with --action move')
    parser.add_argument('--force',
                        help='Force recheck of all files.  By default, we will skip files we have already checked.',
                        action='store_true')
    parser.add_argument('--decompress',
                        help='Extract multipart RARs, and clean up the originals.',
                        action='store_true')
    parser.add_argument('--merge',
                        help='In a directory with subtitles or separate audio tracks, create a MKV with subs and all tracks.',
                        action='store_true')
    args = parser.parse_args()

    # Do some validation to make sure we can work
    if args.action == 'move' and not args.moveto:
        sys.exit("If using the action move, you must specify a moveto directory")
    else:
        if not os.path.exists(args.moveto):
            sys.exit("Directory {} does not exist.".format(args.moveto))

    if not shutil.which('ffmpeg'):
        sys.exit("Needed utility 'ffmpeg' is not installed.")
    if not shutil.which('unrar') and args.decompress:
        sys.exit("Needed utility 'unrar' is not installed.")
    if not shutil.which('mkvmerge') and args.merge:
        sys.exit("Needed utility 'mkvmerge' is not installed.")

    conf_dir = os.path.expanduser("~/.media_validator/")
    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)

    # Default config
    config = {
        'db_path': os.path.join(conf_dir, "inventory.db"),
        'log': os.path.join(conf_dir, "scan.log"),
        'log_level': "INFO",
        'print_log': False,
        'action': None,
        'types': ["mkv", "m4v", "avi", "mov", "avchd", "mpeg", "mp4", "wmv", "mov", "mpg", "ogv", "flv"],
        'force': False,
        'decompress': False,
        'merge': False,
        'torrent_client': {
            'transmission': {
                'address': 'localhost',
                'port': 9091,
                'user': None,
                'password': None
            }
        }
    }

    config_path = os.path.join(conf_dir, 'config.json')
    if os.path.isfile(config_path):
        # Overlay any values set in the config.json
        with open(config_path) as fh:
            config.update(json.load(fh))

    # Override if command line
    config.update(args.__dict__)

    try:
        config['loglevel'] = config['loglevel'].upper()
    except AttributeError:
        config['loglevel'] = logging.INFO

    inv = Inventory(config)

    with integrity.MediaChecker(config) as checker:
        checker.enqueue_path(args.path)
        checker.process_queue(config['action'])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        # TODO: Close the DB, somehow...
        sys.exit(0)
