"""
Utilities module
"""
import logging


def get_logger(instance_name):
    # Create a logger for each module
    logger = logging.getLogger(instance_name)

    # Set the logging level
    logger.setLevel(logging.INFO)

    # Create a console handler and set its level to DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create a formatter and set the format to include the time, name, and message
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s")

    # Add the formatter to the console handler
    ch.setFormatter(formatter)

    # Add the console handler to the logger
    logger.addHandler(ch)
    return logger
