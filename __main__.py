import json
import integrity
import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Recursively check if media is valid.")
    parser.add_argument('path', help="Where to look")
    parser.add_argument('--loglevel', help='Log Level: currently supports debug, info, error, and fatal.  Defaults to info.')
    parser.add_argument('--action', help='Action to perform on failed validations.  Currently only supports delete.  Defaults to none.')
    args = parser.parse_args()

    conf_dir = os.path.expanduser("~/.media_validator/")
    if not os.path.exists(conf_dir):  # TODO: Make this recursive
        os.makedirs(conf_dir)

    # Default config
    config = {
        'db_path': conf_dir + "inventory.db",
        'log': conf_dir + "scan.log",
        'loglevel': "INFO",
        'action': None,
        'types': ["mkv", "m4v", "avi", "mov", "avchd", "mpeg", "mp4", "wmv", "mov"]
    }

    config_path = conf_dir + 'config.json'
    if os.path.isfile(config_path):
        # Overlay any values set in the config.json
        config.update(json.load(config_path))

    # Override if command line
    config['loglevel'] = args.loglevel if 'loglevel' in args else config['loglevel']
    config['action'] = args.action if 'action' in args else config['action']

    with integrity.MediaChecker(config) as checker:
        checker.enqueue_path(args.path)
        checker.process_queue(config['action'])


if __name__ == "__main__":
    main()
