from pystac import Collection, Item, Provider, STAC_EXTENSIONS

from src.sac_stac.domain.extensions import register_product_definition_extension


class SacCollection(Collection):
    def add_providers(self, collection_config: dict):
        providers = []

        for provider in collection_config.get('providers'):
            providers.append(Provider(
                name=provider.get('name'), roles=provider.get('roles'), url=provider.get('url')))

        self.providers = providers

    def add_product_definition_extension(self, product_definition: dict):
        if not STAC_EXTENSIONS.is_registered_extension('product_definition'):
            register_product_definition_extension()
        self.ext.enable('product_definition')
        self.ext.product_definition.metadata_type = product_definition.get('metadata_type')
        self.ext.product_definition.metadata = product_definition.get('metadata')
        self.ext.product_definition.measurements = product_definition.get('measurements')


class SacItem(Item):
    def add_common_metadata(self, common_metadata_config: dict):
        self.common_metadata.gsd = common_metadata_config.get('gsd')
        self.common_metadata.platform = common_metadata_config.get('platform')
        self.common_metadata.instruments = common_metadata_config.get('instruments')
        self.common_metadata.constellation = common_metadata_config.get('constellation')

    def add_extensions(self, extensions_config: dict):
        if extensions_config.get('projection'):
            self.ext.enable('projection')
            self.ext.projection.epsg = extensions_config.get('projection').get('epsg')

        if extensions_config.get('eo'):
            self.ext.enable('eo')
            self.ext.eo.cloud_cover = extensions_config.get('eo').get('cloud_cover')
