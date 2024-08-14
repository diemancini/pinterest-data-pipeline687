from datetime import datetime
import logging
from typing import Dict
import pathlib

import json
import requests
import yaml

logging.basicConfig(
    encoding="utf-8",
    level=logging.INFO,
    format="INFO: [%(asctime)s] - %(message)s",
)
logger = logging.getLogger(f"{datetime.today().strftime("%Y-%m-%d %H:%M:%S")}")


class Utils:

    CURRENT_PATH = pathlib.Path(__file__).parent.resolve()
    DB_CONFIG_FILE = f"{CURRENT_PATH}/../config/db_creds.yaml"
    INVOKE_URL = "https://74y1om8mn3.execute-api.us-east-1.amazonaws.com/dev/topics"
    INVOKE_URL_STREAM = "https://74y1om8mn3.execute-api.us-east-1.amazonaws.com/kinesis"
    #INVOKE_URL_STREAM = "https://74y1om8mn3.execute-api.us-east-1.amazonaws.com/test-stage"

    def __init__(self):
        self.logger = logger

    def read_aws_credentials(self) -> Dict:
        """
        Read config file
        """
        with open(self.DB_CONFIG_FILE, "r") as file:
            config = yaml.safe_load(file)
        return config
    
    def serialize_datetime(self, data: Dict) -> Dict:
        """
        Convert datetime to string in YYYY-mm-DD HH:MM:SS format.

        Parameters:
            - data: Dict
        """
        keys = list(data.keys())
        for key in keys:
            if isinstance(data[key], datetime):
                data[key] = data[key].strftime("%Y-%m-%d %H:%M:%S")
        return data

    def serialize_datetime_put_records(self, data: Dict) -> Dict:
        """
        Convert datetime to string in YYYY-mm-DD HH:MM:SS format.

        Parameters:
            - data: Dict
        """
        if isinstance(data, list):
            keys = list(data[0]["data"].keys())
            logger.info(keys)
            for i in range(len(data)):
                for key in keys:
                    if isinstance(data[i]["data"][key], datetime):
                        #print(data[i]["data"][key])
                        data[i]["data"][key] = data[i]["data"][key].strftime("%Y-%m-%d %H:%M:%S")
        else:
            data = self.serialize_datetime(data)
        return data

    def http_batch(self, data: Dict, topic: str, method="POST") -> None:
        """
        Send data to Gateway API AWS. 
        The payload must be in this format:
                {"records": [
                    {"value": data },
                    {"value": data },
                    ....
                    ....
                    ....
                    ]
                }
        Where data contains the info that it should sent to the server.

        Parameters:
            - data: Dict,
            - topic: string -> Name of topic
            - method: str
        """
        invoke_url = f"{self.INVOKE_URL}/{topic}"
        data = self.serialize_datetime_put_records(data)

        payload = json.dumps({"records": [{"value": data}]})
        #logger.info(payload)

        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        response = requests.request(method, invoke_url, headers=headers, data=payload)
        logger.info(response)
    
    def http_stream(self, data: Dict={}, stream_name: str="", record: str = "record", method="POST") -> None:
        """
        Read/Send data to Kinesis AWS.
        Where data contains the info that it should sent to the server.

        Parameters:
            - data: Dict,
            - stream_name: string -> Name of Kinesis stream.
            - record: str -> Url path for creating or updating data.
            - method: str
        """
        payload = {}
        invoke_url = f"{self.INVOKE_URL_STREAM}/streams/{stream_name}"
        data = self.serialize_datetime_put_records(data)
        if method.upper() == "PUT":
            invoke_url += f"/{record}"
        if method.upper() == "PUT" and record == "records":
            payload = json.dumps({
                "StreamName": stream_name,
                "records": data
            })
        elif method.upper() == "PUT" or method.upper() == "POST":
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": data,
                "PartitionKey": "partition-1"
            })
        logger.info(payload)

        headers =  {'Content-Type': 'application/json'}
        response = requests.request(method, invoke_url, headers=headers, data=payload)
        logger.info(invoke_url)
        logger.info(response)
        logger.info(response.content)
