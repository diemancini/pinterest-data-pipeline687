import json
import logging
from multiprocessing import Process
import random
from time import sleep
from typing import Dict

from utils import Utils
import requests
import boto3
import sqlalchemy
from sqlalchemy import text

random.seed(100)
logging.basicConfig(encoding="utf-8", level=logging.INFO)
logger = logging.getLogger(__name__)


class AWSDBConnector(Utils):

    def __init__(self):

        db_creds = self.read_aws_credentials()
        self.HOST = db_creds["HOST"]
        self.USER = db_creds["USER"]
        self.PASSWORD = db_creds["PASSWORD"]
        self.DATABASE = db_creds["DATABASE"]
        self.PORT = 3306

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            logger.info(f"-" * 40)
            logger.info(pin_result)
            logger.info(geo_result)
            logger.info(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    logger.info("Working")
