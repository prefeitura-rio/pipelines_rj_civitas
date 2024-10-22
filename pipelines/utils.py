# -*- coding: utf-8 -*-
import io
from typing import List

import aiohttp
import discord
import folium
import geopandas as gpd
import pandas as pd
from folium.plugins.beautify_icon import BeautifyIcon

from pipelines.scraping_redes.telegram.models.redis_hash import RedisHash


def generate_png_map(
    locations: list[tuple[float, float]],
    zoom_start: int = 10,
    nearby_cameras: pd.DataFrame = pd.DataFrame(),
):
    """
    Generate a PNG map image as bytes from a list of locations.

    Parameters:
    locations (list[tuple[float, float]]): List of coordinates to plot as markers.
    zoom_start (int, optional): Initial zoom level of the map. Defaults to 10.
    nearby_cameras (pd.DataFrame, optional): Dataframe containing the nearby cameras information.
    Defaults to pd.DataFrame().

    Returns:
    bytes: The PNG image as bytes of the map.
    """

    if not locations:
        return None

    latitude, longitude = locations[0]  # Center the map on the first location
    map = folium.Map(location=[latitude, longitude], zoom_start=zoom_start, control_scale=True)

    # Add markers
    for lat, lon in locations:
        folium.Marker(location=[lat, lon], popup="Marker", icon=folium.Icon(color="red")).add_to(
            map
        )

    if not nearby_cameras.empty:
        camera_locations = []
        cameras_markers: List[folium.Marker] = plot_nearby_cameras(nearby_cameras)

        for camera in cameras_markers:
            camera.add_to(map)
            camera_locations.append((camera.location[0], camera.location[1]))

        # Fit the map to the camera locations and the main point
        if camera_locations:
            camera_locations.append((latitude, longitude))  # Add main point to the list
            map.fit_bounds(camera_locations)

    # Capturar o map como PNG em memória
    img_data = map._to_png(1)
    return img_data


def plot_nearby_cameras(
    df_nearby_cameras: pd.DataFrame,
) -> List:
    """
    Generate a list of folium markers of nearby cameras .

    Parameters:
        df_nearby_cameras (pd.DataFrame): Dataframe with columns 'id_ocorrencia',
        'latitude', 'longitude', 'id_camera', 'nome'.

    Returns:
        List of folium.Marker: List of markers to be added to a folium map.
    """
    if df_nearby_cameras.empty:
        return []

    cameras_gdf = gpd.GeoDataFrame(
        df_nearby_cameras,
        geometry=gpd.points_from_xy(
            x=df_nearby_cameras.longitude,
            y=df_nearby_cameras.latitude,
            crs="EPSG:4326",  # or: crs = pyproj.CRS.from_user_input(4326)
        ),
    )

    cameras_markers = []
    for idx, row in cameras_gdf.iterrows():
        cameras_markers.append(
            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                popup=f"Camera: {idx}",
                icon=BeautifyIcon(
                    icon="",
                    icon_shape="marker",
                    background_color="green",
                    border_color="yellow",
                    number=str(row["rn"]),
                    text_color="white",
                    inner_icon_style="font-size:16px;",
                    icon_size=[35, 35],
                ),
            )
        )

    return cameras_markers


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
