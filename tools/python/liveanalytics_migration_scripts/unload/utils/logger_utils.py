import logging 
import os

def create_logger(logger_name, log_file=None, log_level=logging.INFO):
    """
    Create a logger that can log to both console and file.
    
    Args:
        logger_name (str): Name of the logger
        log_file (str, optional): Path to log file. If None, logs only to console
        log_level: Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger object
    """
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    
    # Clear any existing handlers (to avoid duplicate logs)
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # If log_file is specified, create file handler
    if log_file:
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Create file handler and set level
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    return logger