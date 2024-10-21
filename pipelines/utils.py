# -*- coding: utf-8 -*-
import io

import aiohttp
import discord
import folium

from pipelines.scraping_redes.telegram.models.redis_hash import RedisHash


def generate_png_map(locations: list[tuple[float, float]], zoom_start: int = 10):
    """
    Generate a PNG map using Folium given a latitude and longitude.

    Args:
        latitude (float): The latitude of the point.
        longitude (float): The longitude of the point.

    Returns:
        bytes: The PNG image data.
    """

    if not locations:
        raise ValueError("Locations list cannot be empty.")

    latitude, longitude = locations[0]  # Center the map on the first location
    map = folium.Map(location=[latitude, longitude], zoom_start=zoom_start, control_scale=True)

    # Add markers
    for lat, lon in locations:
        folium.Marker(location=[lat, lon], popup="Marker", icon=folium.Icon(color="red")).add_to(
            map
        )

    img_data = map._to_png(1)
    return img_data


async def send_discord_message(
    webhook_url: str,
    message: str,
    image_data: bytes = None,
):
    """Send a message to a Discord webhook.

    Args:
        webhook_url (str): The URL of the webhook.
        message (str): The message to send.
        image_data (bytes): The PNG image data to embed.
    """
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)
        if image_data:
            file = discord.File(io.BytesIO(image_data), filename="image.png")
            await webhook.send(content=message, file=file)
        else:
            await webhook.send(content=message)


def get_redis_client(
    host: str,
    port: int,
    db: int,  # pylint: disable=C0103
    password: str,
) -> RedisHash:
    """
    Returns a Redis client.
    """
    return RedisHash(
        host=host,
        port=port,
        db=db,
        password=password,
    )
