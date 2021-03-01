from shapely.geometry import Polygon
from datetime import datetime
from rasterio.crs import CRS

from src.sac_stac.domain.operations import get_geometry_from_raster, obtain_date_from_filename, get_bands_from_assets
from src.sac_stac.util import get_files_from_dir


def test_obtain_date_from_filename():
    date = obtain_date_from_filename(
        file='tests/data/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/S2A_MSIL2A_20151022T222102_T01KBU_B01_60m.tif',
        regex='(\d{8}T\d{6})',
        date_format='%Y%m%dT%H%M%S')
    assert date == datetime(2015, 10, 22, 22, 21, 2)


def test_obtain_date_from_filename_s3_url():
    date = obtain_date_from_filename(
        file='s3://public-eo-data/common_sensing/fiji/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/'
             'S2A_MSIL2A_20151022T222102_T01KBU_B01_60m.tif',
        regex='(\d{8}T\d{6})',
        date_format='%Y%m%dT%H%M%S')
    assert date == datetime(2015, 10, 22, 22, 21, 2)


def test_obtain_date_from_filename_http_url():
    date = obtain_date_from_filename(
        file='http://s3-uk-1.sa-catapult.co.uk/public-eo-data/common_sensing/fiji/sentinel_2/'
             'S2A_MSIL2A_20151022T222102_T01KBU/S2A_MSIL2A_20151022T222102_T01KBU_B01_60m.tif',
        regex='(\d{8}T\d{6})',
        date_format='%Y%m%dT%H%M%S')
    assert date == datetime(2015, 10, 22, 22, 21, 2)


def test_get_geometry_from_raster():
    file = 'tests/data/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/S2A_MSIL2A_20151022T222102_T01KBU_AOT_10m.tif'

    # Open raster file as bytes
    with open(file, "rb") as r:
        raster = r.read()

    geometry, crs = get_geometry_from_raster(raster)

    assert geometry == Polygon(
        [(309780, 7790200), (309780, 7900000), (199980, 7900000), (199980, 7790200), (309780, 7790200)])
    assert crs == CRS.from_user_input(32701)


def test_get_bands_from_assets():
    assets = get_files_from_dir('tests/data/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU', 'tif')
    bands = get_bands_from_assets(assets)

    assert bands == ['B01_60m', 'B03_10m', 'SCL_20m', 'B09_60m', 'B02_10m', 'B11_20m', 'B12_20m', 'B08_10m',
                     'B04_10m', 'B07_20m', 'B8A_20m', 'AOT_10m', 'B06_20m', 'WVP_10m', 'B05_20m']
