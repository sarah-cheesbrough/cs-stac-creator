import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Tuple

from rasterio import MemoryFile, RasterioIOError
from rasterio.crs import CRS
from shapely.geometry import box, Polygon

from src.sac_stac.load_config import LOG_LEVEL, LOG_FORMAT
from src.sac_stac.util import extract_common_prefix


logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

logger = logging.getLogger(__name__)


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


def get_projection_from_raster(raster: bytes) -> Tuple[list, list]:
    try:
        with MemoryFile(raster) as raster_file:
            with raster_file.open() as ds:
                return list(ds.shape), list(ds.transform)
    except RasterioIOError as e:
        logger.warning(e)
        return [], []


def get_bands_from_product_keys(product_keys: list) -> list:
    product_names = [Path(a).stem for a in product_keys]
    common_prefix = extract_common_prefix(product_names)
    return [b.replace(common_prefix, '') for b in product_names]
