from utils import Utils

import sqlalchemy
from sqlalchemy import text, Engine


class AWSDBConnector(Utils):

    def __init__(self):

        db_creds = self.read_aws_credentials()
        self.HOST = db_creds["HOST"]
        self.USER = db_creds["USER"]
        self.PASSWORD = db_creds["PASSWORD"]
        self.DATABASE = db_creds["DATABASE"]
        self.PORT = 3306

    def create_db_connector(self) -> Engine:
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine
