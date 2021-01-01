import sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
import json

import pymysql


def lambda_handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """
    from main import main
    try:
        main()
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not complete update.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")