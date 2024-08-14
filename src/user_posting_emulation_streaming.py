import random
from typing import Dict
from utils import Utils
from aws_db_connector import AWSDBConnector

from sqlalchemy import text

logger = Utils().logger


new_connector = AWSDBConnector()


def run_infinite_post_data_loop(max_size: int = 1) -> Dict:

    # while True:
    pin_result = []
    geo_result = []
    user_result = []
    for i in range(max_size):
        # sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            # pin_string = text(f"SELECT * FROM pinterest_data ORDER BY RAND() LIMIT 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                # pin_result = dict(row._mapping)
                pin_result.append(dict(row._mapping))

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            # geo_string = text(f"SELECT * FROM geolocation_data ORDER BY RAND() LIMIT 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                # geo_result = dict(row._mapping)
                geo_result.append(dict(row._mapping))

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            # user_string = text(f"SELECT * FROM user_data ORDER BY RAND() LIMIT 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                # user_result = dict(row._mapping)
                user_result.append(dict(row._mapping))

            # new_connector.http(pin_result, "0affcd87e38f.pin")
            # new_connector.http(geo_result, "0affcd87e38f.geo")
            # new_connector.http(user_result, "0affcd87e38f.user")

            logger.info("{} {} {}".format("-" * 40, i + 1, "-" * 40))
            logger.info(f"pin -> {pin_result[i]['index']}")
            logger.info(f"geo -> {geo_result[i]['ind']}")
            logger.info(f"user -> {user_result[i]['ind']}")

    return {"pin": pin_result, "geo": geo_result, "user": user_result}


if __name__ == "__main__":
    dict_result = run_infinite_post_data_loop(2)
    # print(dict_result["pin"][0])
    data_pin = [
        {"data": row, "partition-key": f"partition-{i+1}"}
        for i, row in enumerate(dict_result["pin"])
    ]
    data_geo = [
        {"data": row, "partition-key": f"partition-{i+1}"}
        for i, row in enumerate(dict_result["geo"])
    ]
    data_user = [
        {"data": row, "partition-key": f"partition-{i+1}"}
        for i, row in enumerate(dict_result["user"])
    ]
    # logger.info(len(data_pin))
    new_connector.http_stream(
        data=data_pin,
        stream_name="streaming-0affcd87e38f-pin",
        record="records",
        method="PUT",
    )
    new_connector.http_stream(
        data=data_geo,
        stream_name="streaming-0affcd87e38f-geo",
        record="records",
        method="PUT",
    )
    new_connector.http_stream(
        data=data_user,
        stream_name="streaming-0affcd87e38f-user",
        record="records",
        method="PUT",
    )

    # new_connector.http_stream(
    #     dict_result["pin"], "streaming-0affcd87e38f-pin", method="PUT"
    # )
    # new_connector.http_stream(
    #     dict_result["geo"], "streaming-0affcd87e38f-geo", method="PUT"
    # )
    # new_connector.http_stream(
    #     dict_result["user"], "streaming-0affcd87e38f-user", method="PUT"
    # )
