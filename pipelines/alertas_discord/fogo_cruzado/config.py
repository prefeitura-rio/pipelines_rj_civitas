# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, List

import pandas as pd


class MessageManager:
    def __init__(self):
        """
        Initialize the MessageManager object.

        The MessageManager stores messages to be sent to Discord, where the keys
        are occurrence_id and the values are dictionaries containing the keys
        'content', 'nearby_cameras', 'timestamp_message', and 'map'.
        """

        self.messages: dict[str, dict] = {}

    def add_message(
        self,
        occurrence_id: str,
        content: str = None,
        nearby_cameras: List = [],
        bytes_map: bytes = None,
        timestamp_message: datetime = None,
    ):
        """
        Add a new message to the MessageManager.

        Parameters:
            occurrence_id (str): The occurrence_id of the message.
            content (str): The content of the message.
            nearby_cameras (List): A list of nearby cameras.
            timestamp_message (datetime): The timestamp of the message.

        Returns:
            None
        """

        self.messages[occurrence_id] = {
            "content": content,
            "nearby_cameras": nearby_cameras,
            "timestamp_message": timestamp_message,
            "map": bytes_map,
        }

    def get_message(self, occurrence_id: str) -> dict:
        """
        Get a message by occurrence_id.

        Parameters:
            occurrence_id (str): The occurrence_id of the message.

        Returns:
            dict: A dictionary containing the keys 'content', 'nearby_cameras',
            'timestamp_message', and 'map'.
        """
        return self.messages.get(occurrence_id, {})

    def update_message(self, occurrence_id: str, key: str, value: Any):
        """
        Update a message in the MessageManager.

        Parameters:
            occurrence_id (str): The occurrence_id of the message.
            key (str): The key of the message to update.
            value (Any): The value to update the key with.

        Returns:
            None
        """
        if occurrence_id in self.messages:
            self.messages[occurrence_id][key] = value

    def update_multiple_messages(self, occurrence_id: str, updates: list):
        """
        Update multiple keys of a message in the MessageManager.

        Parameters:
            occurrence_id (str): The occurrence_id of the message.
            updates (list): A list of dictionaries, where each dictionary contains
            the keys 'key' and 'value'.
                'key' is the key of the message to update.
                'value' is the value to update the key with.

        Returns:
            None
        """

        for param in updates:
            self.update_message(occurrence_id, param["key"], param["value"])

    def get_all_messages(self) -> dict:
        """
        Get all messages stored in the MessageManager.

        Returns:
            dict: A dictionary containing all messages, where the keys are occurrence_id
            and the values are dictionaries containing the keys 'content', 'nearby_cameras',
            'timestamp_message', and 'map'.
        """
        return self.messages


class Config:
    """
    Configuration class that holds the parameters and data used in the workflow.
    """

    def __init__(self, start_datetime: str, webhook_url: dict, reasons: List[str]):
        """
        Initialize the Config object.

        Parameters
        ----------
        start_datetime : str
            The start datetime to query the database.
        webhook_url : dict
            The webhook URL to send the messages to.

        """

        self.start_datetime = start_datetime
        self.webhook_url = webhook_url
        self.reasons = reasons
        self.newest_occurrences = pd.DataFrame()
        self.message_manager = MessageManager()
