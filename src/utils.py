from typing import Dict
import pathlib

import yaml


class Utils:

    CURRENT_PATH = pathlib.Path(__file__).parent.resolve()
    DB_CONFIG_FILE = f"{CURRENT_PATH}/../config/db_creds.yaml"

    def read_aws_credentials(self) -> Dict:
        """
        Read config file
        """
        with open(self.DB_CONFIG_FILE, "r") as file:
            config = yaml.safe_load(file)
        return config
