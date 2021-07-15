# utilities to explore the geolifeclef 2021 dataset

# the data from the kaggle page should be stored within
# the data directory

import os
import pandas as pd

import utils

GLC_DATA = "geolifeclef-2021"
RASTER_DATA = "TM_WORL_BORDERS-0.3"
ENVIROMENTAL = "data/metadata/environmental_variables.csv"
LANDCOVER = "data/metadata/landcover_original_labels.csv"
# LANDCOVER = "data/metadata/landcover_suggested_alignment.csv" 


def print_base_hierarchy(verbose=False):
    """ Retrieve basic structure of the geolifeclef dataset dicrectory. """
    # utils.load_dirs() # loads data_dir and parent_dir globally
    global glc_dir, rasters_dir
    glc_dir = os.path.join(utils.data_dir, GLC_DATA)
    rasters_dir = os.path.join(utils.data_dir, RASTER_DATA)
    if verbose: return
    for file in [dr for dr in os.listdir(glc_dir) if not "DS" in dr]:
        print(f"- {file}")
        if os.path.isdir(os.path.join(glc_dir, file)):
            [print(f"|- {dr}") for dr in os.listdir(
                os.path.join(glc_dir, file)) if not "DS" in dr]
    return

def print_metadata(landcover=False):
    """ Load and retrieve the metadata. """
    data = ENVIROMENTAL if not landcover else LANDCOVER
    if "glc_dir" not in globals():
        print_base_hierarchy(verbose=True)
    meta_df = pd.read_csv(os.path.join(glc_dir, data), sep=";")
    return meta_df



def main():
    print_base_hierarchy()

if __name__ == "__main__":
    main()
