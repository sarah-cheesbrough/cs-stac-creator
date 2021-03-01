from pathlib import Path
from typing import List

from src.sac_stac.domain.s3 import S3


class S3Repository:

    def __init__(self, s3: S3):
        self.s3 = s3

    def get_datasets(self, bucket: str, sensor_prefix: str) -> List[str]:
        return self.s3.list_common_prefixes(bucket_name=bucket, prefix=sensor_prefix)

    def get_measurements(self, bucket: str, dataset_prefix: str) -> List[str]:
        return [Path(o.key).stem for o in self.s3.list_objects(bucket_name=bucket, prefix=dataset_prefix, suffix='.tif')]

    def get_smallest_measurement_sample(self, bucket: str, dataset_prefix: str) -> bytes:
        measurements = self.s3.list_objects(bucket_name=bucket, prefix=dataset_prefix, suffix='.tif')
        measurements_size = {m.size: m.key for m in measurements if m.size > 1}
        min_size = min(list(measurements_size.keys()))
        return self.s3.get_object_body(bucket_name=bucket, object_name=measurements_size.get(min_size))
