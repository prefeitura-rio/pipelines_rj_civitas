# -*- coding: utf-8 -*-
from typing import List, Literal

from pipelines.constants import constants
from pipelines.scraping_redes.telegram.utils import base64_to_file, build_redis_name
from pipelines.utils import get_redis_client


class Pipeline:
    project_id: str = None
    dataset_id: str = None
    table_id: str = None
    mode: Literal["dev", "prod"] = None
    channels_names: List[str] = None
    redis_client = None
    redis_password: str = None

    telegram_secrets: dict = {}
    channels_last_dates: dict = {}

    @classmethod
    def initialize(
        cls,
        project_id: str,
        dataset_id: str,
        table_id: str,
        mode: Literal["dev", "prod"],
        channels_names: List[str],
        redis_password: str,
        telegram_secrets: dict,
    ):
        """
        Initializes the pipeline configuration with the provided parameters.

        Args:
            cls: The class object.
            project_id (str): The ID of the project.
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table.
            mode (str): The mode of the pipeline (dev or prod).
            channels_names (List[str]): The list of channel names.
            redis_password (str): The password for Redis.
            telegram_secrets (dict): The dictionary containing Telegram secrets.

        Returns:
            None
        """
        cls.project_id = project_id
        cls.dataset_id = dataset_id
        cls.table_id = table_id
        cls.mode = mode
        cls.channels_names = channels_names
        cls.redis_client = get_redis_client(
            host=constants.RJ_CIVITAS_REDIS_HOST.value,
            port=constants.RJ_CIVITAS_REDIS_PORT.value,
            db=constants.RJ_CIVITAS_REDIS_DB.value,
            password=redis_password,
        )
        cls.redis_password = redis_password
        cls.telegram_secrets = telegram_secrets

        cls.create_session_file()
        cls.get_channels_last_dates()

    @classmethod
    def set_redis_name(cls, name: str):
        """
        Constructs a Redis key name based on the dataset ID, table ID, given name
        and mode.

        Args:
            cls: The class object.
            name (str): The name to be appended to the Redis key.

        Returns:
            str: The constructed Redis key name.
        """
        redis_name = build_redis_name(
            dataset_id=cls.dataset_id, table_id=cls.table_id, name=name, mode=cls.mode
        )
        return redis_name

    @classmethod
    def get_channels_last_dates(cls):
        """
        Retrieves the last dates of messages from each channel stored in Redis.

        This method retrieves a dictionary from Redis where the keys are the
        channel names and the values are the dates of the last message fetched
        from each channel.

        The dictionary is then stored in the class attribute `channels_last_dates`.

        This method is called during the initialization of the class.
        """
        redis_name = cls.set_redis_name("last_dates")

        response = cls.redis_client.hgetall(redis_name)
        cls.channels_last_dates = response

    @classmethod
    def update_channels_last_dates(cls, last_dates: dict[str, str]):
        """Updates the last dates of messages on Redis from each channel given in the dictionary.

        Args:
            last_dates (dict[str, str]): A dictionary where the keys are the
                channel names and the values are the dates of the last message
                fetched from each channel.
        """
        redis_name = cls.set_redis_name("last_dates")

        cls.redis_client.hset(name=redis_name, mapping=last_dates)

    @classmethod
    def create_session_file(cls):
        """
        Creates a session file based on the base64 string stored in Redis
        given by the TELEGRAM_SESSION key.

        This method is called during the initialization of the class.
        """
        base64_string: str = cls.telegram_secrets.get("TELEGRAM_SESSION")

        base64_to_file(
            base64_string=base64_string, file_path=f"{constants.SESSION_NAME.value}.session"
        )
