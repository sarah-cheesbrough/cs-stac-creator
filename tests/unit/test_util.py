from sac_stac.util import unparse_s3_url


def test_unparse_s3_url():
    s3_url = 'https://s3-uk-1.sa-catapult.co.uk/public-eo-data/common_sensing/fiji/sentinel_2/'
    product_key = 'common_sensing/fiji/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/' \
                  'S2A_MSIL2A_20151022T222102_T01KBU_B02_10m.tif'

    product_url = unparse_s3_url(s3_url, product_key)

    assert product_url == 'https://s3-uk-1.sa-catapult.co.uk/public-eo-data/' \
                          'common_sensing/fiji/sentinel_2/S2A_MSIL2A_20151022T222102_T01KBU/' \
                          'S2A_MSIL2A_20151022T222102_T01KBU_B02_10m.tif'