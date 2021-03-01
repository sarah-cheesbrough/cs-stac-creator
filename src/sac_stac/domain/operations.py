import re
from datetime import datetime
from pathlib import Path
from typing import Tuple

import rasterio
from pystac import Collection, STAC_EXTENSIONS, Provider, Asset, MediaType, Item
from pystac.extensions.eo import Band
from rasterio import MemoryFile
from rasterio.crs import CRS
from shapely.geometry import box, Polygon

from src.sac_stac.domain.extensions import register_product_definition_extension
from src.sac_stac.util import extract_common_prefix


def obtain_date_from_filename(file: str, regex: str, date_format: str) -> datetime:
    """
    Return date from given file based on regular expression and date format.

    :param file: path to file
    :param regex: regular expression to search in filename
    :param date_format: format used when converting to datetime

    :return: datetime object with obtained date.
    """
    filename = Path(file).name
    match_date = re.search(regex, filename)
    date = None

    if match_date:
        date = datetime.strptime(match_date.group(0), date_format)

    return date


def get_geometry_from_raster(raster: bytes) -> Tuple[Polygon, CRS]:
    with MemoryFile(raster) as raster_file:
        with raster_file.open() as ds:
            geom = box(*ds.bounds)
            crs = ds.crs
    return geom, crs


def get_bands_from_assets(assets: list) -> list:
    asset_names = [Path(a).stem for a in assets]
    common_prefix = extract_common_prefix(asset_names)
    return [b.replace(common_prefix, '') for b in asset_names]


def add_product_definition_extension_to_collection(collection: Collection, product_definition: dict):
    if not STAC_EXTENSIONS.is_registered_extension('product_definition'):
        register_product_definition_extension()
    collection.ext.enable('product_definition')
    collection.ext.product_definition.metadata_type = product_definition.get('metadata_type')
    collection.ext.product_definition.metadata = product_definition.get('metadata')
    collection.ext.product_definition.measurements = product_definition.get('measurements')


def add_providers_to_collection(collection: Collection, collection_config: dict):
    providers = []

    for provider in collection_config.get('providers'):
        providers.append(Provider(
            name=provider.get('name'), roles=provider.get('roles'), url=provider.get('url')))

    collection.providers = providers


def add_common_metadata_to_item(item: Item, common_metadata_config: dict):
    item.common_metadata.gsd = common_metadata_config.get('gsd')
    item.common_metadata.platform = common_metadata_config.get('platform')
    item.common_metadata.instruments = common_metadata_config.get('instruments')
    item.common_metadata.constellation = common_metadata_config.get('constellation')


def add_extensions_to_item(item: Item, extensions_config: dict):
    if extensions_config.get('projection'):
        item.ext.enable('projection')
        item.ext.projection.epsg = extensions_config.get('projection').get('epsg')

    if extensions_config.get('eo'):
        item.ext.enable('eo')
        item.ext.eo.cloud_cover = extensions_config.get('eo').get('cloud_cover')


def get_asset_spatial(img_uri):
    with rasterio.open(img_uri) as ds:
        return list(ds.shape), list(ds.transform)


def add_assets_to_item(item: Item, asset_names: list, collection_config: dict):

    mapped_bands = collection_config.get('extensions').get('eo').get('bands')

    bands = map(mapped_bands.get, get_bands_from_asset_name(asset_names))

    for href, band in zip(asset_names, bands):
        asset = Asset(href=href, media_type=MediaType.COG)

        proj_shp, proj_tran = get_asset_spatial(href)  # TODO: Is this needed?
        # Set Projection
        item.ext.projection.set_transform(proj_tran, asset)
        item.ext.projection.set_shape(proj_shp, asset)
        # Set bands
        item.ext.eo.set_bands([Band.create(
            name=band, description='TBD', common_name=band)],
            asset
        )

        item.add_asset(
            key=band,
            asset=asset
        )
