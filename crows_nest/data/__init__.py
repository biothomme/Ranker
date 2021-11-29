import os
from intake import open_catalog

__CATALOG_NAME = "catalog.yaml"

# we obtain path of the current directory,
# which is where the catalog resides.
catalog_dir = os.path.dirname(os.path.abspath(__file__))

# we load the catalog
cat = open_catalog(os.path.join(catalog_dir, __CATALOG_NAME))