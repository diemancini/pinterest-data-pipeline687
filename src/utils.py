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
    INVOKE_URL = f"https://74y1om8mn3.execute-api.us-east-1.amazonaws.com/dev/topics"

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
        keys = list(data[0].keys())
        for i in range(len(data)):
            for key in keys:
                if isinstance(data[i][key], datetime):
                    data[i][key] = data[i][key].strftime("%Y-%m-%d %H:%M:%S")
        return data

    def http(self, data: Dict, topic: str, method="POST") -> None:
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
        data = self.serialize_datetime(data)

        payload = json.dumps({"records": [{"value": data}]})
        #logger.info(payload)

        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        response = requests.request(method, invoke_url, headers=headers, data=payload)
        logger.info(response)
