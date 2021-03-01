from pystac import STAC_EXTENSIONS
from pystac.extensions.base import CollectionExtension
from pystac.extensions.base import ExtendedObject
from pystac.extensions.base import ExtensionDefinition
from pystac.collection import Collection


class Extensions:
    PRODUCT_DEFINITION = 'product_definition'


class ProdefColExt(CollectionExtension):
    def __init__(self, collection):
        self.collection = collection

    def apply(self, metadata_type=None, metadata=None, measurements=None):
        self.metadata_type = metadata_type
        self.metadata = metadata
        self.measurements = measurements

    @property
    def metadata_type(self):
        """"ADD DOCSTRING!"""
        return self.collection.properties.get('product_definition:metadata_type')

    @metadata_type.setter
    def metadata_type(self, v):
        self.collection.properties['product_definition:metadata_type'] = v

    @property
    def metadata(self):
        """"ADD DOCSTRING!"""
        return self.collection.properties.get('product_definition:metadata')

    @metadata.setter
    def metadata(self, v):
        self.collection.properties['product_definition:metadata'] = v

    @property
    def measurements(self):
        """"ADD DOCSTRING!"""
        return self.collection.properties.get('product_definition:measurements')

    @measurements.setter
    def measurements(self, v):
        self.collection.properties['product_definition:measurements'] = v

    @classmethod
    def from_collection(self, collection):
        return ProdefColExt(collection)

    @classmethod
    def _object_links(cls):
        return []


def register_product_definition_extension():
    extended_object = ExtendedObject(Collection, ProdefColExt)
    extension_definition = ExtensionDefinition(Extensions.PRODUCT_DEFINITION, [extended_object])
    STAC_EXTENSIONS.add_extension(extension_definition)
