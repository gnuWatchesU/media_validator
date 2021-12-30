import logging

def setup_logger(conf, logger):
    logger.setLevel(conf['log_level'])

