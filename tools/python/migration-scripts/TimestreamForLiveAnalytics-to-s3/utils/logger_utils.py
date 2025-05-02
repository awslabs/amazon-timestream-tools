import logging 

def create_logger(logger_name, log_level=logging.INFO):
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    # Create formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(ch)

    return logger