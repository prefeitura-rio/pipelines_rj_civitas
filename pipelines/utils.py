# -*- coding: utf-8 -*-
import io
from typing import List

import aiohttp
import basedosdados as bd
import discord
import folium
import geopandas as gpd
import pandas as pd
from folium.plugins.beautify_icon import BeautifyIcon


def generate_png_map(
    locations: list[tuple[float, float]],
    zoom_start: int = 10,
    nearby_cameras: pd.DataFrame = pd.DataFrame(),
) -> bytes:
    """Generates a PNG map image as bytes from a list of locations.

    Args:
        locations (list[tuple[float, float]]): List of coordinates to plot as markers.
        zoom_start (int, optional): Initial zoom level of the map. Defaults to 10.
        nearby_cameras (pd.DataFrame, optional): DataFrame containing nearby cameras information.
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

    if len(nearby_cameras) > 0:
        camera_locations = []
        cameras_markers: List[folium.Marker] = plot_nearby_cameras(nearby_cameras)

        for camera in cameras_markers:
            camera.add_to(map)
            camera_locations.append((camera.location[0], camera.location[1]))

        # Fit the map to the camera locations and the main point
        if camera_locations:
            camera_locations.append((latitude, longitude))  # Add main point to the list
            map.fit_bounds(camera_locations)

    # Capture the map as PNG in memory
    img_data = map._to_png(1)
    return img_data


def get_nearby_cameras(
    project_id: str,
    dataset_id: str,
    table_id: str,
    id_column: str,
    latitude_column: str,
    longitude_column: str,
    ids: List[str],
) -> pd.DataFrame:
    """Gets the 5 nearest cameras for each id in ids list.

    Args:
        project_id (str): Project ID.
        dataset_id (str): Dataset ID.
        table_id (str): Table ID.
        id_column (str): Name of the ID column.
        latitude_column (str): Name of the latitude column.
        longitude_column (str): Name of the longitude column.
        ids (List[str]): List of occurrence IDs.

    Returns:
        pd.DataFrame: DataFrame containing the 5 nearest cameras for each occurrence.
    """
    if not ids:
        return pd.DataFrame()

    occurrences_ids = "', '".join(ids)

    query_nearby_cameras = f"""
        WITH occurrences AS (
            SELECT
                {id_column} AS id_ocorrencia,
                {latitude_column} AS latitude,
                {longitude_column} AS longitude
            FROM
                `{project_id}.{dataset_id}.{table_id}`
            WHERE
                {id_column} IN ('{occurrences_ids}')
        ),
        distances AS (
            SELECT DISTINCT
                b.id_ocorrencia,
                a.latitude,
                a.longitude,
                a.id_camera,
                a.nome,
                ST_DISTANCE(
                    ST_GEOGPOINT(a.longitude, a.latitude),
                    ST_GEOGPOINT(b.longitude, b.latitude)
                ) AS distance_meters
            FROM
                `rj-cetrio.ocr_radar.cameras` a
            CROSS JOIN
                occurrences b
        )
        SELECT
            d.*,
            ROW_NUMBER() OVER(PARTITION BY id_ocorrencia ORDER BY distance_meters) AS rn
        FROM
            distances d
        QUALIFY(rn) <= 5
        ORDER BY
            id_ocorrencia,
            distance_meters
    """

    df_nearby_cameras: pd.DataFrame = bd.read_sql(query_nearby_cameras)

    return df_nearby_cameras


def plot_nearby_cameras(
    df_nearby_cameras: pd.DataFrame,
) -> List[folium.Marker]:
    """Generates a list of folium markers for nearby cameras.

    Args:
        df_nearby_cameras (pd.DataFrame): DataFrame with columns 'id_ocorrencia',
            'latitude', 'longitude', 'id_camera', 'nome'.

    Returns:
        List[folium.Marker]: List of markers to be added to a folium map.
    """
    if df_nearby_cameras.empty:
        return []

    cameras_gdf = gpd.GeoDataFrame(
        df_nearby_cameras,
        geometry=gpd.points_from_xy(
            x=df_nearby_cameras.longitude,
            y=df_nearby_cameras.latitude,
            crs="EPSG:4326",
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
    """Sends a message to a Discord webhook.

    Args:
        webhook_url (str): The URL of the webhook.
        message (str): The message to send.
        image_data (bytes, optional): The PNG image data to embed.
    """
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(webhook_url, session=session)
        if image_data:
            file = discord.File(io.BytesIO(image_data), filename="image.png")
            await webhook.send(content=message, file=file)
        else:
            await webhook.send(content=message)
