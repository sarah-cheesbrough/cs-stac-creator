import json
from pathlib import Path

from geopandas import GeoSeries
from moto import mock_s3
from pystac import Catalog, Extent, TemporalExtent, SpatialExtent, Asset, MediaType
from pystac.extensions.eo import Band

from sac_stac.adapters import repository
from sac_stac.domain.model import SacCollection, SacItem
from sac_stac.domain.operations import obtain_date_from_filename, get_geometry_from_cog, \
    get_bands_from_product_keys, get_projection_from_cog
from sac_stac.domain.s3 import S3
from sac_stac.load_config import config
from sac_stac.util import parse_s3_url, load_json


def initialise_s3_bucket(sensor_name, s3_resource, bucket_name):
    s3_resource.create_bucket(Bucket=bucket_name)
    for file in Path(f'tests/data/common_sensing/fiji/{sensor_name}').glob('**/*.tif'):
        s3_resource.Bucket(bucket_name).upload_file(
            Filename=str(file),
            Key=f"common_sensing/fiji/{sensor_name}/{file.parent.stem}/{file.name}"
        )


@mock_s3
def test_stac_files_creation():

    catalog = Catalog(
        id=config.get('id'),
        title=config.get('title'),
        description=config.get('description'),
        stac_extensions=config.get('stac_extensions')
    )

    items = []
    for sensor in config.get('sensors'):
        collection = SacCollection(
            id=sensor.get('id'),
            title=sensor.get('title'),
            description=sensor.get('description'),
            extent=Extent(SpatialExtent([[]]), TemporalExtent([[]])),
            properties={}
        )
        collection.add_providers(sensor)
        collection.add_product_definition_extension(
            product_definition=sensor.get('extensions').get('product_definition'),
            bands_metadata=sensor.get('extensions').get('eo').get('bands')
        )

        # Get sensor url to S3 bucket
        sensor_url = sensor.get('s3_url')
        bucket, sensor_key = parse_s3_url(sensor_url)

        # Initialise S3 client and bucket
        s3 = S3(key=None, secret=None, s3_endpoint=None, region_name='us-east-1')
        initialise_s3_bucket(sensor_name=sensor.get('id'), s3_resource=s3.s3_resource, bucket_name=bucket)

        # Use S3 repository to obtain acquisition keys
        repo = repository.S3Repository(s3)
        acquisition_keys = repo.get_acquisition_keys(bucket=bucket, acquisition_prefix=sensor_key)

        for acquisition_key in acquisition_keys:
            # Get date from acquisition name
            date_format = sensor.get('formatting').get('date').get('format')
            date_regex = sensor.get('formatting').get('date').get('regex')
            date = obtain_date_from_filename(
                file=acquisition_key,
                regex=date_regex,
                date_format=date_format)

            # Get sample product and extract geometry
            product_sample = repo.get_smallest_product_key(bucket=bucket, products_prefix=acquisition_key)
            #                  ** ONLY FOR TESTING **
            product_sample = f'tests/data/{product_sample}'
            geometry, crs = get_geometry_from_cog(product_sample)

            item = SacItem(
                id=Path(acquisition_key).stem,
                datetime=date,
                geometry=json.loads(GeoSeries([geometry], crs=crs).to_crs(4326).to_json()).get('features')[0].get(
                    'geometry'),
                bbox=list(geometry.bounds),
                properties={}
            )

            item.add_extensions(sensor.get('extensions'))
            item.add_common_metadata(sensor.get('common_metadata'))

            bands_metadata = sensor.get('extensions').get('eo').get('bands')
            mapped_bands = {b.get('name'): b.get('common_name') for b in bands_metadata}
            product_keys = repo.get_product_keys(bucket=bucket, products_prefix=acquisition_key)
            bands = map(mapped_bands.get, get_bands_from_product_keys(product_keys))

            for product_key, band in zip(product_keys, bands):
                asset = Asset(
                    href=product_key,
                    media_type=MediaType.COG
                )

                # Set Projection                                         ** ONLY FOR TESTING **
                proj_shp, proj_tran = get_projection_from_cog(f'tests/data/{product_key}')
                item.ext.projection.set_transform(proj_tran, asset)
                item.ext.projection.set_shape(proj_shp, asset)

                # Set bands
                item.ext.eo.set_bands([Band.create(
                    name=band, description='TBD', common_name=band)],
                    asset
                )

                item.add_asset(key=band, asset=asset)

            items.append(item)
            collection.add_item(item)

        collection.update_extent_from_items()
        catalog.add_child(collection)

    catalog.normalize_hrefs(config.get('output_url'))
    catalog.validate_all()

    assert catalog.to_dict() == load_json('tests/output/catalog.json')
    for sensor in config.get('sensors'):
        assert collection.to_dict() == load_json(f"tests/output/{sensor.get('id')}/collection.json")
    for item in items:
        assert item.to_dict() == load_json(f'tests/output/{item.collection_id}/{item.id}/{item.id}.json')
