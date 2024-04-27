import sys
# append the path of the parent directory
sys.path.append("./")

import os
import time
import json
import traceback
from kafka import KafkaProducer
from dotenv import load_dotenv
from loguru import logger as LOGGER
from config import BROKER_EXTERNAL_PORT
from extract_reddit import collect_submission_details, collect_subreddit_details
load_dotenv()

TIME_SLEEP = 20

def main():

    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=f'localhost:{BROKER_EXTERNAL_PORT}'
    )
    
    
    while True:
        try:
            submission_details = collect_submission_details(os.getenv("SUBREDDIT_NAME"))   
            for submission_info in submission_details:
                message = json.dumps(submission_info)
                LOGGER.debug(f"Submission info: {message}")
                producer.send("reddit-submissions", message.encode("utf-8"))
            producer.flush()

            # submission_details = collect_subreddit_details(os.getenv("SUBREDDIT_NAME"))
            # message = json.dumps(submission_details)
            # LOGGER.debug(f"Submission details: {message}")
            # producer.send("reddit-subreddit", message.encode("utf-8"))
            # producer.flush()

            LOGGER.info("Produced subreddit details")
        except Exception as _:
            LOGGER.error(f"An error occurred while retrieving from reddit: {traceback.format_exc()}")
        
        LOGGER.info(f"Production done, sleeping for {TIME_SLEEP} seconds...")
        time.sleep(TIME_SLEEP)
        LOGGER.info("Starting over again") 

if __name__ == "__main__":
    main()


