# -*- coding: utf-8 -*-
from typing import List

import pandas as pd


class Message:
    def __init__(
        self,
        chat_id,
        chat_username,
        chat_name,
        message_id,
        timestamp,
        text,
        media,
        views,
        geolocalizacao,
    ):
        self.chat_id = chat_id
        self.chat_username = chat_username
        self.chat_name = chat_name
        self.message_id = message_id
        self.timestamp = timestamp
        self.text = text
        self.media = media
        self.views = views
        self.geolocalizacao = geolocalizacao

    def to_dict(self):
        return {
            "chat_id": self.chat_id,
            "chat_username": self.chat_username,
            "chat_name": self.chat_name,
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "text": self.text,
            "media": self.media,
            "views": self.views,
            "geolocalizacao": self.geolocalizacao,
        }


class Channel:
    messages: dict[str, List[Message]] = {}

    def __init__(self, nome):
        self.nome = nome
        self.messages = []

        self.update_channels(**{nome: self.messages})

    @classmethod
    def update_channels(cls, **kwargs):
        cls.messages.update(kwargs)

    def add_message(self, message: Message):
        Channel.messages[self.nome].append(message)

    def len_mensagens(self) -> int:
        """
        Returns the number of messages in this channel.

        Returns
        -------
        int
            The number of messages in the channel.
        """
        return len(Channel.messages.get(self.nome, []))

    @classmethod
    def len_class_mensagens(cls, channels: List[str] = None) -> int:
        """
        Returns the total number of messages across specified channels.

        Parameters
        ----------
        channels : List[str], optional
            List of channel names to calculate total messages for.
            If None, calculates for all channels.

        Returns
        -------
        int
            Total number of messages across the specified channels.
        """
        if not channels:
            channels = cls.messages.keys()

        messages_quantity = 0
        if channels:
            messages_length = [len(cls.messages.get(channel, [])) for channel in channels]
            messages_quantity = sum(messages_length)

        return messages_quantity

    @classmethod
    def to_dict_list(cls) -> List[dict]:
        """
        Returns a list of dictionaries, where each dictionary represents a message
        from Telegram channel in the format returned by Message.to_dict().

        Returns
        -------
        List[Dict]
        """
        if not cls.messages:
            return []

        data = [msg.to_dict() for msg_list in cls.messages.values() for msg in msg_list]

        return data

    def get_max_message_date(self, tz: str = None) -> str:
        """
        Returns the latest date from the messages in this channel.

        Parameters
        ----------
        tz : str, optional
            The timezone to convert to. Defaults is None.
            If a timezone is provided, it will be converted from UTC to the specified timezone.

        Returns
        -------
        str
            The latest date in the format "%Y-%m-%d %H:%M:%S".
        """
        if not self.messages:
            return None

        last_date = max(message.timestamp for message in self.messages)

        if tz:
            last_date = pd.to_datetime(last_date, format="%Y-%m-%d %H:%M:%S", utc=True)
            last_date = last_date.tz_convert(tz)
            last_date = last_date.strftime("%Y-%m-%d %H:%M:%S")

        return last_date
