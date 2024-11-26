# -*- coding: utf-8 -*-
import time
from typing import Dict, List, Literal

import requests
from prefeitura_rio.pipelines_utils.logging import log
from tenacity import retry, stop_after_attempt, wait_exponential


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
    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True
    )
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
            # Awaits 1 second before the request
            time.sleep(1)

            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            response_data = response.json().get("data", [])

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log("Rate limit reached. Awaiting 15 seconds before trying again...")
                time.sleep(15)
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                response_data = response.json().get("data", [])
            else:
                log(f"Error getting chats: {e}")
                raise e
        except Exception as e:
            log(f"Error getting chats: {e}")
            raise e

        return response_data

    @classmethod
    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True
    )
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

        data = []

        # first request
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            total_pages = response.json().get("meta", {}).get("totalPages", 1)
            data.extend(response.json().get("data", []))

            # Awaits 1 second between requests
            time.sleep(1)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log("Rate limit reached. Awaiting 15 seconds before trying again...")
                time.sleep(15)
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                total_pages = response.json().get("meta", {}).get("totalPages", 1)
                data.extend(response.json().get("data", []))
            else:
                log(f"Error getting messages: {e}")
                raise e
        except Exception as e:
            log(f"Error getting messages: {e}")
            raise e

        for i in range(2, total_pages + 1):
            try:
                params["page"] = i
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data.extend(response.json().get("data", []))

                time.sleep(1)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    log(
                        f"Rate limit reached on page {i}. Awaiting 15 seconds before trying again..."
                    )
                    time.sleep(15)
                    response = requests.get(url, params=params, headers=headers)
                    response.raise_for_status()
                    data.extend(response.json().get("data", []))
                else:
                    log(f"Error getting messages on page {i}: {e}")
                    raise e
            except Exception as e:
                log(f"Error getting messages on page {i}: {e}")
                raise e

        return data
