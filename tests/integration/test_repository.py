from moto import mock_s3
from src.sac_stac.adapters import repository
from src.sac_stac.domain.s3 import S3
from pathlib import Path

BUCKET = 'public-eo-data'


def initialise_cs_bucket(s3_resource, bucket_name):
    s3_resource.create_bucket(Bucket=bucket_name)
    for file in Path('tests/data/sentinel_2').glob('**/*.tif'):
        s3_resource.Bucket(bucket_name).upload_file(
            Filename=str(file),
            Key=f"common_sensing/fiji/sentinel_2/{file.parent.stem}/{file.name}"
        )


@mock_s3
def test_get_acquisitions_from_sensor():
    s3 = S3(key=None, secret=None, s3_endpoint=None, region_name='us-east-1')
    initialise_cs_bucket(s3_resource=s3.s3_resource, bucket_name=BUCKET)

    repo = repository.S3Repository(s3)
    acquisitions = repo.get_acquisitions_from_sensor(bucket=BUCKET, sensor_prefix='common_sensing/fiji/sentinel_2/')

    assert acquisitions == ['common_sensing/fiji/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/',
                            'common_sensing/fiji/sentinel_2/S2B_MSIL2A_20191023T220919_T01KBA/',
                            'common_sensing/fiji/sentinel_2/S2B_MSIL2A_20191023T220919_T01KBB/']


@mock_s3
def test_get_products_from_acquisition():
    s3 = S3(key=None, secret=None, s3_endpoint=None, region_name='us-east-1')
    initialise_cs_bucket(s3_resource=s3.s3_resource, bucket_name=BUCKET)

    repo = repository.S3Repository(s3)
    products = repo.get_products_from_acquisition(bucket=BUCKET,
                                                  acquisitions_prefix='common_sensing/fiji/sentinel_2/'
                                                                      'S2A_MSIL2A_20151022T222102_T01KBU/')

    assert products == ['S2A_MSIL2A_20151022T222102_T01KBU_AOT_10m', 'S2A_MSIL2A_20151022T222102_T01KBU_B01_60m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B02_10m', 'S2A_MSIL2A_20151022T222102_T01KBU_B03_10m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B04_10m', 'S2A_MSIL2A_20151022T222102_T01KBU_B05_20m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B06_20m', 'S2A_MSIL2A_20151022T222102_T01KBU_B07_20m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B08_10m', 'S2A_MSIL2A_20151022T222102_T01KBU_B09_60m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B11_20m', 'S2A_MSIL2A_20151022T222102_T01KBU_B12_20m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_B8A_20m', 'S2A_MSIL2A_20151022T222102_T01KBU_SCL_20m',
                        'S2A_MSIL2A_20151022T222102_T01KBU_WVP_10m']


@mock_s3
def test_get_smallest_product_sample():
    s3 = S3(key=None, secret=None, s3_endpoint=None, region_name='us-east-1')
    initialise_cs_bucket(s3_resource=s3.s3_resource, bucket_name=BUCKET)

    repo = repository.S3Repository(s3)
    product = repo.get_smallest_product_sample(bucket=BUCKET,
                                               acquisitions_prefix='common_sensing/fiji/sentinel_2/'
                                                                   'S2A_MSIL2A_20151022T222102_T01KBU/')

    file = 'tests/data/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/S2A_MSIL2A_20151022T222102_T01KBU_B02_10m.tif'

    # Open raster file as bytes
    with open(file, "rb") as r:
        raster = r.read()

    assert product == raster
