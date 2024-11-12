# -*- coding: utf-8 -*-
from typing import Dict, List, Literal

import requests
from prefeitura_rio.pipelines_utils.logging import log


class Palver:
    """
    Classe para extração de dados do Palver
    """

    __base_url = None
    __token = None

    @classmethod
    def set_token(cls, token: str):
        cls.__token = token

    @classmethod
    def set_base_url(cls, url: str):
        cls.__base_url = url

    @classmethod
    def get_chats(
        cls,
        source_name: str,
        query: str = "*",
        page: int = 1,
        page_size: int = 10,
        sort_order: Literal["asc", "desc"] = "desc",
        sort_field: Literal["participants", "name"] = None,
        chat_id: str = None,
        group_name: str = None,
    ) -> dict:

        # if not cls.check_token_expired():
        #     raise Exception('Authentication failed')

        url = f"{cls.__base_url}/{source_name}/chats"
        params = {
            "query": query,
            "page": page,
            "perPage": page_size,
            "sortOrder": sort_order,
            "sortField": sort_field,
            "id": chat_id,
            "name": group_name,
        }
        headers = {"Authorization": f"Bearer {cls.__token}"}

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            response_data = response.json().get("data", [])

        except Exception as e:
            log(f"Error getting chats: {e}")
            raise e
        return response_data

    @classmethod
    def get_messages(
        cls,
        source_name: str,
        query: str = "*",
        page: int = 1,
        page_size: int = 10,
        sort_order: Literal["asc", "desc"] = "desc",
        sort_field: Literal["participants", "name"] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> List[Dict]:

        url = f"{cls.__base_url}/{source_name}/messages"
        params = {
            "query": query,
            "page": page,
            "perPage": page_size,
            "sortOrder": sort_order,
            "sortField": sort_field,
            "startDate": start_date,
            "endDate": end_date,
        }
        headers = {"Authorization": f"Bearer {cls.__token}"}

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])
            log(f"{len(data)} messages found")
        except Exception as e:
            log(f"Error getting chats: {e}")
            raise e
        return data


if __name__ == "__main__":
    # palver = Palver()
    # palver.set_base_url("https://api.telegram.org/bot{token}/getUpdates")
    # palver.set_token("654321")
    # palver.get_messages("civitas")
    pass
